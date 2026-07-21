//! Structured, colored logging for the DEX ingestor, matching the
//! shared ZORA log format: `HH:MM:SS.mmm  LEVEL  DEX  key=value ...`.
//!
//! Deliberately not built on the `log`/`env_logger` crates: their
//! `Formatter` API assumes one pre-formatted message string per log
//! record, which doesn't fit this format's per-field (key vs value)
//! coloring without fragile string-splitting at print time. This
//! module is small enough to own directly instead, so both crates were
//! removed from Cargo.toml.
use std::io::IsTerminal;
use std::sync::OnceLock;

pub enum Level {
    Info,
    Warn,
    Error,
}

const RESET: &str = "\x1b[0m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";
const GREEN: &str = "\x1b[32m";
const GRAY: &str = "\x1b[90m";
const BRIGHT_WHITE: &str = "\x1b[97m";

// Checked once and cached: colors degrade to plain text if stdout isn't
// a terminal (e.g. redirected to a file) or $TERM says the terminal
// doesn't support them (unset, empty, or "dumb"). A tmux pane is a
// real pty, so `is_terminal()` is true there -- colors work under tmux.
fn colors_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::io::stdout().is_terminal()
            && std::env::var("TERM")
                .map(|t| !t.is_empty() && t != "dumb")
                .unwrap_or(false)
    })
}

fn level_str(level: &Level) -> &'static str {
    match level {
        Level::Info => "INFO ",
        Level::Warn => "WARN ",
        Level::Error => "ERROR",
    }
}

// The `status` field is colored by its own content (a small state
// machine: CONNECTING/CONNECTED/ERROR) rather than the fixed
// gray-key/white-value rule every other field uses.
fn status_color(status: &str) -> &'static str {
    match status {
        "CONNECTED" => GREEN,
        "ERROR" => RED,
        _ => YELLOW, // CONNECTING / RECONNECTING
    }
}

/// HH:MM:SS.mmm  LEVEL  DEX  key=value key=value...
///
/// `fields` values are `&str` rather than anything numeric: format
/// numbers at the call site (`&format!("{:.3}", x)` works fine here --
/// see the note in main.rs about why a few small allocations in the
/// logging path, as opposed to the price/SHM-write path, are fine).
pub fn log(level: Level, fields: &[(&str, &str)]) {
    let time_str = chrono::Local::now().format("%H:%M:%S%.3f");
    let colors = colors_enabled();
    let lvl_str = level_str(&level);

    let level_part = if !colors {
        lvl_str.to_string()
    } else {
        match level {
            Level::Warn => format!("{YELLOW}{lvl_str}{RESET}"),
            Level::Error => format!("{RED}{lvl_str}{RESET}"),
            Level::Info => lvl_str.to_string(),
        }
    };

    let source_part = if colors {
        format!("{GREEN}DEX{RESET}")
    } else {
        "DEX".to_string()
    };

    let mut message = String::new();
    for (i, (key, value)) in fields.iter().enumerate() {
        if i > 0 {
            message.push(' ');
        }
        if colors {
            let value_color = if *key == "status" {
                status_color(value)
            } else {
                BRIGHT_WHITE
            };
            message.push_str(&format!("{GRAY}{key}={value_color}{value}{RESET}"));
        } else {
            message.push_str(&format!("{key}={value}"));
        }
    }

    println!("{time_str}  {level_part}  {source_part}  {message}");
}

/// Startup banner, printed once. Not part of the structured key=value
/// log format -- it's decorative box art, not a telemetry line.
pub fn print_banner(title: &str, subtitle: &str) {
    let colors = colors_enabled();
    let (green, yellow, reset) = if colors {
        (GREEN, YELLOW, RESET)
    } else {
        ("", "", "")
    };

    println!("{green}┌─────────────────────────────────┐{reset}");
    println!("{green}│{reset}  {title:<30} {green}│{reset}");
    println!("{green}│{reset}  {subtitle:<30} {green}│{reset}");
    println!(
        "{green}│{reset}  Status: {yellow}{:<23}{reset}{green}│{reset}",
        "CONNECTING..."
    );
    println!("{green}└─────────────────────────────────┘{reset}");
}
