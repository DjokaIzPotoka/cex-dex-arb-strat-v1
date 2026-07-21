#include "ipc/shm_writer.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstring>

namespace zora::ipc {

ShmWriter::~ShmWriter() {
    shutdown();
}

bool ShmWriter::init(const char* shm_name, const char* symbol) noexcept {
    shm_name_ = shm_name;
    fd_ = shm_open(shm_name_, O_CREAT | O_RDWR, 0666);
    if (fd_ == -1) return false;

    // On macOS, ftruncate on a POSIX shared memory object can only
    // succeed once, ever -- calling it again on a segment that already
    // has a size (e.g. one left behind by a process that was killed
    // before it could shm_unlink) fails with EINVAL, even if resizing
    // to the same size. Only truncate when the existing segment is
    // too small for what we need; a pre-existing, already-correctly-
    // sized segment (the common case after an unclean shutdown) is
    // reused as-is. Linux has no such restriction, so this check is a
    // no-op cost there, not a workaround needed there specifically.
    struct stat st{};
    if (fstat(fd_, &st) == -1 || static_cast<std::size_t>(st.st_size) < ZORA_SHM_SIZE) {
        if (ftruncate(fd_, static_cast<off_t>(ZORA_SHM_SIZE)) == -1) {
            close(fd_);
            fd_ = -1;
            return false;
        }
    }

    void* addr = mmap(nullptr, ZORA_SHM_SIZE, PROT_READ | PROT_WRITE,
                       MAP_SHARED, fd_, 0);
    if (addr == MAP_FAILED) {
        close(fd_);
        fd_ = -1;
        return false;
    }

    shm_ = static_cast<ZoraCexPrice*>(addr);
    std::memset(shm_, 0, sizeof(ZoraCexPrice));

    std::size_t n = std::strlen(symbol);
    if (n >= sizeof(shm_->symbol)) n = sizeof(shm_->symbol) - 1;
    std::memcpy(shm_->symbol, symbol, n);
    shm_->symbol[n] = '\0';

    std::atomic_store_explicit(&shm_->sequence, 0u, std::memory_order_release);

    owns_segment_ = true;
    seq_counter_ = 0;
    return true;
}

uint32_t ShmWriter::write(double bid, double ask, uint64_t timestamp_us) noexcept {
    shm_->bid = bid;
    shm_->ask = ask;
    shm_->mid = (bid + ask) * 0.5;
    shm_->timestamp_us = timestamp_us;

    ++seq_counter_;
    std::atomic_store_explicit(&shm_->sequence, seq_counter_, std::memory_order_release);
    return seq_counter_;
}

void ShmWriter::signal_gap() noexcept {
    seq_counter_ = 0;
    std::atomic_store_explicit(&shm_->sequence, 0u, std::memory_order_release);
}

void ShmWriter::shutdown() noexcept {
    if (shm_ != nullptr) {
        munmap(shm_, ZORA_SHM_SIZE);
        shm_ = nullptr;
    }
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
    if (owns_segment_) {
        shm_unlink(shm_name_);
        owns_segment_ = false;
    }
}

} // namespace zora::ipc
