//! Structured, colored logging for the spread detector, matching the
//! shared ZORA log format: `HH:MM:SS.mmm  LEVEL  SPR  key=value ...`.
//!
//! Not built on the `log`/`env_logger` crates, same reasoning as the
//! DEX ingestor's logger.rs: their `Formatter` API assumes one
//! pre-formatted message string per record, which doesn't fit this
//! format's per-field (and per-value-content) coloring without
//! fragile string-splitting at print time.
use std::io::IsTerminal;
use std::sync::OnceLock;

pub enum Level {
    Info,
    Warn,
    Error,
}

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";
const MAGENTA: &str = "\x1b[35m"; // SPR's identity color (CEX=cyan, DEX=green)
const BRIGHT_GREEN: &str = "\x1b[92m";
const GRAY: &str = "\x1b[90m";
const BRIGHT_WHITE: &str = "\x1b[97m";

// Checked once and cached: colors degrade to plain text if stdout
// isn't a terminal, or $TERM says it doesn't support them (unset,
// empty, or "dumb"). A tmux pane is a real pty, so this is true there.
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

/// Per the spec: `status=WATCHING` is default color, `status=OPPORTUNITY`
/// is bright green + bold; `net=+X%` (positive) is green, `net=-X%`
/// (negative) is left at the default color -- highlighting the common
/// case (no opportunity) would just be noise. Everything else uses the
/// standard gray-key/white-value rule.
fn special_value_color(key: &str, value: &str) -> Option<&'static str> {
    match key {
        "status" if value == "OPPORTUNITY" => Some(BRIGHT_GREEN),
        "net" if value.starts_with('+') => Some(BRIGHT_GREEN),
        _ => None,
    }
}

/// HH:MM:SS.mmm  LEVEL  SPR  key=value key=value...
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
        format!("{MAGENTA}SPR{RESET}")
    } else {
        "SPR".to_string()
    };

    let mut message = String::new();
    for (i, (key, value)) in fields.iter().enumerate() {
        if i > 0 {
            message.push(' ');
        }
        if !colors {
            message.push_str(&format!("{key}={value}"));
            continue;
        }
        match special_value_color(key, value) {
            // status=OPPORTUNITY additionally gets bold, since it's
            // the single most important word this process ever prints.
            Some(c) if *key == "status" => {
                message.push_str(&format!("{GRAY}{key}={RESET}{BOLD}{c}{value}{RESET}"));
            }
            Some(c) => {
                message.push_str(&format!("{GRAY}{key}={c}{value}{RESET}"));
            }
            None => {
                message.push_str(&format!("{GRAY}{key}={BRIGHT_WHITE}{value}{RESET}"));
            }
        }
    }

    println!("{time_str}  {level_part}  {source_part}  {message}");
}

/// Startup banner, printed once. Not part of the structured key=value
/// log format -- it's decorative box art, not a telemetry line.
pub fn print_banner(title: &str, subtitle: &str) {
    let colors = colors_enabled();
    let (magenta, yellow, reset) = if colors {
        (MAGENTA, YELLOW, RESET)
    } else {
        ("", "", "")
    };

    println!("{magenta}┌─────────────────────────────────┐{reset}");
    println!("{magenta}│{reset}  {title:<30} {magenta}│{reset}");
    println!("{magenta}│{reset}  {subtitle:<30} {magenta}│{reset}");
    println!(
        "{magenta}│{reset}  Status: {yellow}{:<23}{reset}{magenta}│{reset}",
        "CONNECTING..."
    );
    println!("{magenta}└─────────────────────────────────┘{reset}");
}

const OPPORTUNITY_BOX_WIDTH: usize = 42; // interior content width, between the ║ borders

/// Builds the "OPPORTUNITY DETECTED" box as a string (one line per
/// `Vec` entry, no trailing newlines) -- split out from
/// `print_opportunity_box` so the fixed-width padding can be unit
/// tested directly instead of only by eyeballing printed output.
fn format_opportunity_box(
    cex_bid: f64,
    dex_price: f64,
    gross_pct: f64,
    net_pct: f64,
    green: &str,
    bold: &str,
    reset: &str,
) -> Vec<String> {
    let w = OPPORTUNITY_BOX_WIDTH;
    let lines = [
        "OPPORTUNITY DETECTED".to_string(),
        // 8 decimals, not 3: BONK/USDC's price (~0.0000031) needs it --
        // 3 decimals would print "0.000" here. The box's padding is
        // width-driven (see the loop below), so this doesn't affect
        // alignment for the larger-priced pairs, just adds trailing
        // digits.
        format!("CEX bid:    {:.8}", cex_bid),
        format!("DEX price:  {:.8}", dex_price),
        format!("Gross:      {:+.3}%", gross_pct),
        format!("Net:        {:+.3}%", net_pct),
    ];

    let mut out = Vec::with_capacity(7);
    out.push(format!("{green}{bold}╔{}╗{reset}", "═".repeat(w)));
    out.push(format!("{green}{bold}║  {:<width$}║{reset}", lines[0], width = w - 2));
    out.push(format!("{green}{bold}╠{}╣{reset}", "═".repeat(w)));
    for line in &lines[1..] {
        out.push(format!("{green}{bold}║  {:<width$}║{reset}", line, width = w - 2));
    }
    out.push(format!("{green}{bold}╚{}╝{reset}", "═".repeat(w)));
    out
}

/// The decorative "OPPORTUNITY DETECTED" box, shown only when
/// net_spread > the configured threshold. Like the banner, this is
/// decorative box art rather than a structured log line.
pub fn print_opportunity_box(cex_bid: f64, dex_price: f64, gross_pct: f64, net_pct: f64) {
    let colors = colors_enabled();
    let (green, bold, reset) = if colors {
        (BRIGHT_GREEN, BOLD, RESET)
    } else {
        ("", "", "")
    };

    for line in format_opportunity_box(cex_bid, dex_price, gross_pct, net_pct, green, bold, reset)
    {
        println!("{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opportunity_box_lines_are_uniform_width() {
        // No color codes (empty strings) so the raw character count
        // reflects exactly what a plain terminal would show.
        let lines = format_opportunity_box(76.802, 76.650, 0.198, -0.227, "", "", "");
        let expected_len = OPPORTUNITY_BOX_WIDTH + 2; // + the two border chars
        for (i, line) in lines.iter().enumerate() {
            assert_eq!(
                line.chars().count(),
                expected_len,
                "line {i} has wrong width: {line:?}"
            );
        }
    }
}
