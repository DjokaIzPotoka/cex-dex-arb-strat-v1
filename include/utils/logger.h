#pragma once
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <initializer_list>
#include <unistd.h>

namespace zora::utils {

enum class LogLevel { Info, Warn, Error };

// One key=value field in a structured log line. `value` must already be
// formatted into a string (typically a small stack buffer at the call
// site via snprintf) -- the logger itself does no numeric formatting or
// allocation, keeping it safe to call from the hot path.
struct Field {
    const char* key;
    const char* value;
};

namespace detail {

constexpr const char* kReset = "\033[0m";
constexpr const char* kYellow = "\033[33m";
constexpr const char* kRed = "\033[31m";
constexpr const char* kCyan = "\033[36m";
constexpr const char* kGreen = "\033[32m";
constexpr const char* kGray = "\033[90m";
constexpr const char* kBrightWhite = "\033[97m";

// Checked once and cached: colors degrade to plain text if stdout isn't
// a terminal (e.g. redirected to a file) or $TERM says the terminal
// doesn't support them (unset, empty, or "dumb"). A tmux pane is a real
// pty, so isatty() is true there -- colors work fine under tmux.
inline bool colors_enabled() {
    static const bool enabled = [] {
        if (!isatty(fileno(stdout))) return false;
        const char* term = std::getenv("TERM");
        if (term == nullptr || term[0] == '\0') return false;
        if (std::strcmp(term, "dumb") == 0) return false;
        return true;
    }();
    return enabled;
}

inline const char* level_str(LogLevel lvl) {
    switch (lvl) {
        case LogLevel::Info:  return "INFO ";
        case LogLevel::Warn:  return "WARN ";
        case LogLevel::Error: return "ERROR";
    }
    return "?????";
}

// The `status=` field's value is colored by its own content rather than
// the fixed gray-key/white-value rule every other field uses, since it
// is itself a small state machine (CONNECTING/CONNECTED/RECONNECTING/
// ERROR) that's more useful to scan by color than by reading the word.
inline const char* status_color(const char* status) {
    if (std::strcmp(status, "CONNECTED") == 0) return kGreen;
    if (std::strcmp(status, "ERROR") == 0) return kRed;
    return kYellow; // CONNECTING / RECONNECTING
}

inline void write_time(char* buf, std::size_t buf_size) {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto ms_total = duration_cast<milliseconds>(now.time_since_epoch()).count();
    auto secs = static_cast<std::time_t>(ms_total / 1000);
    auto millis = static_cast<int>(ms_total % 1000);

    std::tm tm_buf{};
    localtime_r(&secs, &tm_buf);

    std::snprintf(buf, buf_size, "%02d:%02d:%02d.%03d",
                  tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec, millis);
}

} // namespace detail

// HH:MM:SS.mmm  LEVEL  CEX  key=value key=value...
inline void log(LogLevel level, std::initializer_list<Field> fields) {
    using namespace detail;

    char time_buf[16];
    write_time(time_buf, sizeof(time_buf));

    const bool colors = colors_enabled();
    const char* lvl_str = level_str(level);

    if (colors && level == LogLevel::Warn) {
        std::fprintf(stdout, "%s  %s%s%s  ", time_buf, kYellow, lvl_str, kReset);
    } else if (colors && level == LogLevel::Error) {
        std::fprintf(stdout, "%s  %s%s%s  ", time_buf, kRed, lvl_str, kReset);
    } else {
        std::fprintf(stdout, "%s  %s  ", time_buf, lvl_str);
    }

    if (colors) {
        std::fprintf(stdout, "%sCEX%s  ", kCyan, kReset);
    } else {
        std::fprintf(stdout, "CEX  ");
    }

    bool first = true;
    for (const auto& f : fields) {
        if (!first) std::fprintf(stdout, " ");
        first = false;

        if (!colors) {
            std::fprintf(stdout, "%s=%s", f.key, f.value);
            continue;
        }

        const char* value_color =
            (std::strcmp(f.key, "status") == 0) ? status_color(f.value) : kBrightWhite;
        std::fprintf(stdout, "%s%s=%s%s%s", kGray, f.key, value_color, f.value, kReset);
    }
    std::fprintf(stdout, "\n");
}

// Convenience for a message with no fields (e.g. "Shutdown complete").
inline void log_line(LogLevel level, const char* message) {
    log(level, {{"msg", message}});
}

// Startup banner, printed once. Not part of the structured key=value
// log format -- it's decorative box art, not a telemetry line, so it's
// plain fprintf rather than going through log(). `subtitle` is the
// second line inside the box (e.g. "Binance SOL/USDC  bookTicker").
inline void print_banner(const char* title, const char* subtitle) {
    const bool colors = detail::colors_enabled();
    const char* cyan = colors ? detail::kCyan : "";
    const char* yellow = colors ? detail::kYellow : "";
    const char* reset = colors ? detail::kReset : "";

    std::fprintf(stdout, "%s┌─────────────────────────────────┐%s\n", cyan, reset);
    std::fprintf(stdout, "%s│%s  %-30s %s│%s\n", cyan, reset, title, cyan, reset);
    std::fprintf(stdout, "%s│%s  %-30s %s│%s\n", cyan, reset, subtitle, cyan, reset);
    std::fprintf(stdout, "%s│%s  Status: %s%-23s%s%s│%s\n", cyan, reset, yellow,
                  "CONNECTING...", reset, cyan, reset);
    std::fprintf(stdout, "%s└─────────────────────────────────┘%s\n", cyan, reset);
}

} // namespace zora::utils
