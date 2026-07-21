//! Paper-trading logger: appends every detected opportunity to a CSV
//! file. Paper mode only -- nothing here ever submits a real order.
use std::fs::OpenOptions;
use std::path::Path;

use anyhow::{Context, Result};

use crate::types::Opportunity;

pub struct PaperLogger {
    writer: csv::Writer<std::fs::File>,
}

impl PaperLogger {
    pub fn new(csv_path: &str) -> Result<Self> {
        if let Some(parent) = Path::new(csv_path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create directory for {csv_path}"))?;
            }
        }

        let file_exists = Path::new(csv_path).exists();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(csv_path)
            .with_context(|| format!("failed to open {csv_path}"))?;

        let mut writer = csv::WriterBuilder::new().has_headers(false).from_writer(file);

        if !file_exists {
            writer.write_record([
                "timestamp",
                "pair",
                "cex_bid",
                "dex_price",
                "gross_spread_pct",
                "net_spread_pct",
                "optimal_size_usdc",
                "estimated_profit_usd",
                "direction",
                "cex_seq",
                "dex_seq",
                "dex_slot",
            ])?;
            writer.flush()?;
        }

        Ok(Self { writer })
    }

    /// Appends one row and flushes immediately -- a trade record must
    /// never sit only in a buffer that could be lost if the process
    /// dies before the next flush.
    pub fn log(&mut self, opp: &Opportunity) -> Result<()> {
        self.writer.write_record(&[
            opp.detected_at_us.to_string(),
            opp.pair.to_string(),
            // 8 decimals, not 4: BONK/USDC's price (~0.0000031) needs it --
            // 4 decimals would silently record every BONK row as "0.0000",
            // making the CSV useless for that pair. Harmless for the
            // larger-priced pairs (SOL/RAY/ETH), just more trailing digits.
            format!("{:.8}", opp.cex_bid),
            format!("{:.8}", opp.dex_price),
            format!("{:.4}", opp.gross_spread * 100.0),
            format!("{:.4}", opp.net_spread * 100.0),
            format!("{:.2}", opp.optimal_size_usdc),
            format!("{:.4}", opp.estimated_profit),
            opp.direction.as_str().to_string(),
            opp.cex_seq.to_string(),
            opp.dex_seq.to_string(),
            opp.dex_slot.to_string(),
        ])?;
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Direction;

    fn sample_opportunity() -> Opportunity {
        Opportunity {
            pair: "SOL/USDC",
            cex_bid: 76.802,
            dex_price: 76.650,
            gross_spread: 0.00198,
            net_spread: -0.00227,
            optimal_size_usdc: 487.32,
            expected_sol_out: 6.35,
            estimated_profit: 0.80,
            direction: Direction::BuyDexSellCex,
            detected_at_us: 1_700_000_000_000_000,
            cex_seq: 32,
            dex_seq: 58,
            dex_slot: 432_736_870,
        }
    }

    #[test]
    fn writes_header_once_and_appends_rows() {
        let dir = std::env::temp_dir().join(format!("zora_paper_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("trades.csv");
        let path_str = path.to_str().unwrap();

        {
            let mut logger = PaperLogger::new(path_str).expect("create paper logger");
            logger.log(&sample_opportunity()).expect("log first row");
        }
        // Reopening (as would happen on a process restart) must not
        // duplicate the header.
        {
            let mut logger = PaperLogger::new(path_str).expect("reopen paper logger");
            logger.log(&sample_opportunity()).expect("log second row");
        }

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 3, "expected 1 header + 2 rows, got: {contents}");
        assert!(lines[0].starts_with("timestamp,pair,cex_bid,dex_price"));
        assert!(lines[1].contains("SOL/USDC"));
        assert!(lines[1].contains("BUY_DEX_SELL_CEX"));
        assert!(lines[1].contains("76.80200000")); // cex_bid formatted to 8 decimals

        std::fs::remove_dir_all(&dir).ok();
    }
}
