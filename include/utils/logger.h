#pragma once
#include <chrono>
#include <cstdio>
#include <ctime>

namespace zora::utils {

enum class LogLevel { Info, Book, Shm, Warn, Error };

inline const char* level_str(LogLevel lvl) {
    switch (lvl) {
        case LogLevel::Info:  return "INFO ";
        case LogLevel::Book:  return "BOOK ";
        case LogLevel::Shm:   return "SHM  ";
        case LogLevel::Warn:  return "WARN ";
        case LogLevel::Error: return "ERROR";
    }
    return "?????";
}

// [HH:MM:SS.uuuuuu] [LEVEL] message
inline void log_line(LogLevel lvl, const char* msg) {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto us_total = duration_cast<microseconds>(now.time_since_epoch()).count();
    auto secs = static_cast<std::time_t>(us_total / 1'000'000);
    auto micros = static_cast<int>(us_total % 1'000'000);

    std::tm tm_buf{};
    localtime_r(&secs, &tm_buf);

    char time_buf[32];
    std::snprintf(time_buf, sizeof(time_buf), "%02d:%02d:%02d.%06d",
                  tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec, micros);

    std::fprintf(stdout, "[%s] [%s] %s\n", time_buf, level_str(lvl), msg);
}

// Fixed-size stack buffer only — safe for the hot path (no heap allocation).
template <typename... Args>
inline void log_fmt(LogLevel lvl, const char* fmt, Args... args) {
    char buf[256];
    std::snprintf(buf, sizeof(buf), fmt, args...);
    log_line(lvl, buf);
}

} // namespace zora::utils
