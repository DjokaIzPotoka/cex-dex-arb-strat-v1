//! Owns both read-only POSIX shared memory mappings this process
//! consumes (the CEX and DEX ingestors' published prices).
//!
//! All `unsafe` in this crate is contained here, for the same reason
//! as the DEX ingestor's shm.rs: mmap-ing shared memory and reading it
//! as a struct is inherently outside what the Rust compiler can verify
//! -- the memory isn't allocated by Rust, and another process is
//! actively writing to it concurrently.
use std::ffi::CString;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{bail, Result};

use crate::types::{ZoraCexPrice, ZoraDexPrice};

/// Opens an existing POSIX shared memory segment read-only. Does NOT
/// create it (no O_CREAT) -- creation is the owning ingestor's job;
/// this process only ever consumes.
///
/// SAFETY: the returned pointer is valid for `size` bytes for as long
/// as the mapping is kept (nothing here unmaps it -- that's the
/// caller's responsibility, e.g. `ShmReader::try_reopen` munmapping a
/// superseded mapping), provided `size` matches the actual struct size
/// the writer uses. That invariant is enforced by the `size_of` calls
/// at each call site below, not by this function itself.
unsafe fn open_existing_readonly(name: &str, size: usize) -> Result<*const u8> {
    let cname = CString::new(name).expect("shm name has no interior NUL");

    // SAFETY: `cname` is a valid NUL-terminated C string for the
    // duration of this call. shm_open returns -1 on failure, checked
    // below before `fd` is used for anything.
    let fd = unsafe { libc::shm_open(cname.as_ptr(), libc::O_RDONLY, 0) };
    if fd < 0 {
        bail!(
            "shm_open({name}) failed: {}",
            std::io::Error::last_os_error()
        );
    }

    // SAFETY: `fd` was just checked valid above. PROT_READ matches the
    // O_RDONLY it was opened with -- this process never writes to
    // either segment. The returned pointer is checked against
    // MAP_FAILED before being used for anything.
    let addr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };

    // SAFETY: the fd isn't needed once mapped -- the mapping itself
    // keeps the underlying shared memory object referenced.
    unsafe {
        libc::close(fd);
    }

    if addr == libc::MAP_FAILED {
        bail!("mmap({name}) failed: {}", std::io::Error::last_os_error());
    }

    Ok(addr as *const u8)
}

pub struct ShmReader {
    cex_ptr: *const ZoraCexPrice,
    dex_ptr: *const ZoraDexPrice,
}

// SAFETY: both pointers reference read-only shared memory that this
// process never mutates; there's no interior mutability on the Rust
// side to race on. Only one task ever touches a `ShmReader` at a time
// in this binary (the single poll loop in main.rs), so Send is enough
// -- Sync isn't needed and isn't implemented.
unsafe impl Send for ShmReader {}

impl ShmReader {
    /// Attempts to map both segments for one pair once. `cex_shm_name`/
    /// `dex_shm_name` let one `ShmReader` instance serve any pair
    /// (SOL/USDC, RAY/USDC, ...) -- construct one instance per pair.
    /// Returns `Err` if either segment doesn't exist yet -- not treated
    /// as fatal by the caller, since the spread detector may start
    /// before either ingestor has.
    ///
    /// Deliberately a single attempt with no internal retry loop: the
    /// caller (main.rs) owns retrying, so it can race each wait
    /// against shutdown signals via `tokio::select!` using the same
    /// signal futures it already holds. An earlier version of this
    /// function looped and slept internally, entirely before main()
    /// had registered any signal handling -- meaning Ctrl+C/SIGTERM
    /// were silently unresponsive for as long as an ingestor hadn't
    /// started yet.
    pub fn try_open(cex_shm_name: &str, dex_shm_name: &str) -> Result<Self> {
        // SAFETY: size matches `size_of::<ZoraCexPrice>()` exactly,
        // which is what makes this mapping valid to later dereference
        // as `*const ZoraCexPrice`.
        let cex_ptr = unsafe {
            open_existing_readonly(cex_shm_name, std::mem::size_of::<ZoraCexPrice>())?
                as *const ZoraCexPrice
        };

        // SAFETY: same reasoning as the CEX mapping above, sized for
        // `ZoraDexPrice` instead.
        let dex_ptr = unsafe {
            open_existing_readonly(dex_shm_name, std::mem::size_of::<ZoraDexPrice>())?
                as *const ZoraDexPrice
        };

        Ok(Self { cex_ptr, dex_ptr })
    }

    /// Re-maps both of this pair's segments by name, for when an
    /// ingestor has restarted: it `shm_unlink`s its old segment on
    /// clean shutdown and creates a brand new one under the same name
    /// on the next startup. `shm_unlink` only removes the *name* --
    /// this reader's existing mapping (opened against the old
    /// segment) stays perfectly valid, just frozen, since nothing
    /// forces us to notice the name now points somewhere else. That's
    /// exactly the "stale forever" failure this method exists to
    /// recover from: the caller (main.rs) calls this once staleness
    /// is actually observed, which re-resolves the name and picks up
    /// whichever segment currently lives there.
    ///
    /// Both new mappings are opened *before* either old one is
    /// unmapped, and only swapped in once both succeed. Retiring the
    /// old mappings first and discovering a failure on the second
    /// `open_existing_readonly` call would otherwise leave this
    /// `ShmReader` holding a dangling pointer into already-unmapped
    /// memory -- `read_cex`/`read_dex` would then be reading freed
    /// memory, not just stale data. On any failure, the existing
    /// mappings are left completely untouched (still valid, just
    /// still stale) and the caller is expected to retry later.
    pub fn try_reopen(&mut self, cex_shm_name: &str, dex_shm_name: &str) -> Result<()> {
        // SAFETY: same reasoning as `try_open`'s CEX mapping.
        let new_cex_ptr = unsafe {
            open_existing_readonly(cex_shm_name, std::mem::size_of::<ZoraCexPrice>())?
                as *const ZoraCexPrice
        };

        // SAFETY: same reasoning as `try_open`'s DEX mapping.
        let new_dex_ptr = match unsafe {
            open_existing_readonly(dex_shm_name, std::mem::size_of::<ZoraDexPrice>())
        } {
            Ok(ptr) => ptr as *const ZoraDexPrice,
            Err(e) => {
                // Don't leak the CEX mapping just opened above -- it
                // was never stored anywhere, so nothing else will
                // ever unmap it if we don't do it here before bailing.
                //
                // SAFETY: `new_cex_ptr` was mapped for
                // `size_of::<ZoraCexPrice>()` bytes moments ago by the
                // call above and hasn't been stored or shared anywhere
                // yet, so it's safe (and necessary) to unmap here.
                unsafe {
                    libc::munmap(
                        new_cex_ptr as *mut libc::c_void,
                        std::mem::size_of::<ZoraCexPrice>(),
                    );
                }
                return Err(e);
            }
        };

        // Both new mappings succeeded -- safe to retire the old ones.
        //
        // SAFETY: `self.cex_ptr`/`self.dex_ptr` were mapped for their
        // respective `size_of` sizes, either in `try_open` or by a
        // previous `try_reopen` call, and nothing else holds a
        // reference to them (this method takes `&mut self`).
        unsafe {
            libc::munmap(
                self.cex_ptr as *mut libc::c_void,
                std::mem::size_of::<ZoraCexPrice>(),
            );
            libc::munmap(
                self.dex_ptr as *mut libc::c_void,
                std::mem::size_of::<ZoraDexPrice>(),
            );
        }

        self.cex_ptr = new_cex_ptr;
        self.dex_ptr = new_dex_ptr;
        Ok(())
    }

    /// Sequence-lock read: retries until a consistent (untorn)
    /// snapshot is observed.
    ///
    /// This is deliberately *not* a textbook seqlock: a textbook
    /// seqlock's writer marks the sequence odd before writing fields
    /// and even after, and a reader additionally checks the sequence
    /// is even. Both the CEX and DEX ingestors' writers instead do a
    /// single release-store on `sequence`, once, after all payload
    /// fields are already written in plain (non-atomic) stores -- see
    /// their own shm_writer.cpp / shm.rs. There is no writer-visible
    /// "odd" state to detect, so checking parity here would do
    /// nothing except incorrectly reject roughly half of all
    /// genuinely valid, complete updates. A mismatched before/after
    /// sequence read is already sufficient: it can only differ if a
    /// complete new publish (fields, then the atomic store) happened
    /// while this read was in progress.
    ///
    /// SAFETY: `self.cex_ptr` is always either the mapping from
    /// `try_open` or the most recent successful `try_reopen`, and both
    /// only ever store a pointer after confirming it maps
    /// `size_of::<ZoraCexPrice>()` bytes -- so it's valid to
    /// dereference for as long as `self` holds that particular value.
    pub fn read_cex(&self) -> ZoraCexPrice {
        loop {
            unsafe {
                let seq_ptr = std::ptr::addr_of!((*self.cex_ptr).sequence) as *mut u32;
                let before = AtomicU32::from_ptr(seq_ptr).load(Ordering::Acquire);
                let snapshot = std::ptr::read(self.cex_ptr);
                let after = AtomicU32::from_ptr(seq_ptr).load(Ordering::Acquire);
                if before == after {
                    return snapshot;
                }
            }
            std::hint::spin_loop();
        }
    }

    /// Same pattern as `read_cex`, for the DEX segment.
    ///
    /// SAFETY: same reasoning as `read_cex`, for `self.dex_ptr` /
    /// `ZoraDexPrice` instead.
    pub fn read_dex(&self) -> ZoraDexPrice {
        loop {
            unsafe {
                let seq_ptr = std::ptr::addr_of!((*self.dex_ptr).sequence) as *mut u32;
                let before = AtomicU32::from_ptr(seq_ptr).load(Ordering::Acquire);
                let snapshot = std::ptr::read(self.dex_ptr);
                let after = AtomicU32::from_ptr(seq_ptr).load(Ordering::Acquire);
                if before == after {
                    return snapshot;
                }
            }
            std::hint::spin_loop();
        }
    }
}
