//! Owns the lifecycle of the POSIX shared memory segment used to
//! publish the Raydium pool price to the spread detector.
//!
//! All `unsafe` in this crate is contained here: mmap-ing a POSIX
//! shared memory segment and treating it as a `ZoraDexPrice` is
//! inherently outside what the Rust compiler can check (the memory
//! wasn't allocated by Rust's allocator, and another process may be
//! reading it concurrently), so it can't be expressed safely. Every
//! other file in this crate works with plain owned/borrowed Rust
//! values.
use std::ffi::CString;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{bail, Result};

use crate::types::ZoraDexPrice;

pub struct ShmWriter {
    ptr: *mut ZoraDexPrice,
    fd: libc::c_int,
    seq: u32,
    shm_name: CString,
}

impl ShmWriter {
    /// Creates, sizes, and maps the shared memory segment named
    /// `shm_name` (e.g. `types::ZORA_SHM_DEX_RAY_NAME`). One instance
    /// per trading pair. Must be called once at startup, before
    /// polling begins.
    pub fn new(shm_name: &str) -> Result<Self> {
        // CString::new only fails if the name contains an interior NUL
        // byte, which can't happen for our fixed, hardcoded constants --
        // expect() here is fine per the "only in tests/main setup" rule
        // since this is a startup-time invariant, not a poll-loop path.
        let name = CString::new(shm_name).expect("shm name has no interior NUL");
        let size = std::mem::size_of::<ZoraDexPrice>();

        // SAFETY: `name` is a valid NUL-terminated C string for the
        // duration of this call. shm_open returns -1 on failure, which
        // we check before treating `fd` as valid.
        let fd = unsafe { libc::shm_open(name.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o666) };
        if fd < 0 {
            bail!("shm_open failed: {}", std::io::Error::last_os_error());
        }

        // On macOS, ftruncate on a POSIX shared memory object can only
        // succeed once, ever -- calling it again on a segment that
        // already has a size (e.g. one left behind by a process that
        // was killed before it could shm_unlink) fails with EINVAL,
        // even when resizing to the same size. Only truncate when the
        // existing segment is too small for what we need; a
        // pre-existing, already-correctly-sized segment (the common
        // case after an unclean shutdown) is reused as-is. Linux has
        // no such restriction, so this check is a no-op cost there.
        //
        // SAFETY: `fd` was just checked valid above; `stat_buf` is
        // zero-initialized and fully populated by fstat before we read
        // any field of it.
        let needs_truncate = unsafe {
            let mut stat_buf: libc::stat = std::mem::zeroed();
            if libc::fstat(fd, &mut stat_buf) == -1 {
                true
            } else {
                (stat_buf.st_size as usize) < size
            }
        };

        if needs_truncate {
            // SAFETY: `fd` was just checked valid above.
            let truncated = unsafe { libc::ftruncate(fd, size as libc::off_t) };
            if truncated < 0 {
                let err = std::io::Error::last_os_error();
                unsafe { libc::close(fd) };
                bail!("ftruncate failed: {}", err);
            }
        }

        // SAFETY: `fd` is a valid, correctly-sized shared memory file
        // descriptor at this point. The returned pointer is checked
        // against MAP_FAILED before being used for anything.
        let addr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if addr == libc::MAP_FAILED {
            let err = std::io::Error::last_os_error();
            unsafe { libc::close(fd) };
            bail!("mmap failed: {}", err);
        }

        let ptr = addr as *mut ZoraDexPrice;

        // SAFETY: `ptr` is valid for `size` bytes (just mapped above).
        // POSIX doesn't guarantee a freshly-truncated segment is
        // zero-filled on every platform, so we zero it explicitly
        // before treating it as a `ZoraDexPrice`.
        unsafe { std::ptr::write_bytes(ptr as *mut u8, 0, size) };

        Ok(Self { ptr, fd, seq: 0, shm_name: name })
    }

    /// Hot path: writes the payload fields, then publishes the update
    /// with a release store on `sequence`. Returns the sequence number
    /// written. Infallible -- once mapped, writing to memory this
    /// process already owns cannot fail.
    pub fn write(
        &mut self,
        reserve_sol: u64,
        reserve_usdc: u64,
        price: f64,
        timestamp_us: u64,
        slot: u64,
    ) -> u32 {
        // SAFETY: `self.ptr` was mapped for the lifetime of `self` in
        // `new()`, and `self.ptr` is private -- nothing else in this
        // process can be writing through it concurrently. Writing the
        // payload fields before the atomic sequence store below is
        // what makes the release ordering meaningful to a reader: it
        // guarantees these writes are visible by the time the reader
        // observes the new sequence number.
        unsafe {
            (*self.ptr).reserve_sol = reserve_sol;
            (*self.ptr).reserve_usdc = reserve_usdc;
            (*self.ptr).price = price;
            (*self.ptr).timestamp_us = timestamp_us;
            (*self.ptr).slot = slot;

            self.seq = self.seq.wrapping_add(1);

            // `sequence` is a plain u32 in the struct (matching the
            // C-side layout byte-for-byte), so we view it as an atomic
            // just for this store rather than typing the field itself
            // as AtomicU32 -- AtomicU32 has the same size/alignment as
            // u32, so this is a valid, layout-preserving reinterpret.
            let seq_ptr = std::ptr::addr_of_mut!((*self.ptr).sequence);
            AtomicU32::from_ptr(seq_ptr).store(self.seq, Ordering::Release);
        }
        self.seq
    }

    /// Consumes the writer, triggering `Drop` (and therefore cleanup)
    /// immediately instead of waiting for it to fall out of scope.
    /// Purely a readability aid at the call site in `main.rs`.
    pub fn shutdown(self) {
        drop(self);
    }
}

impl Drop for ShmWriter {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` and `self.fd` were established in `new()`
        // and are only ever touched here and in `write()`; by the time
        // `Drop` runs, nothing else holds a reference to `self`.
        unsafe {
            libc::munmap(
                self.ptr as *mut libc::c_void,
                std::mem::size_of::<ZoraDexPrice>(),
            );
            libc::close(self.fd);
        }

        // SAFETY: `self.shm_name` is a valid NUL-terminated C string,
        // stored at construction time specifically so shutdown can
        // unlink the same name the segment was created with.
        unsafe {
            libc::shm_unlink(self.shm_name.as_ptr());
        }
    }
}
