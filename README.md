# ZORA

CEX-DEX arbitrage bot for SOL/USDC. Three processes work together:

- **CEX ingestor** (`src/`, C++): Binance `solusdc@bookTicker` via
  WebSocket → shared memory at `/zora_cex_price`
- **DEX ingestor** (`ingestor-dex-solana/`): Raydium SOL/USDC pool via
  Solana RPC polling → shared memory at `/zora_dex_price`
- **Spread detector** (`spread-detector/`): reads both shared memory
  segments, detects profitable CEX-DEX spreads, and records them to
  `data/paper_trades.csv` (paper trading only -- no real orders)

## Quick start

```
make build
make start
```

`make start` launches CEX and DEX in the background (logging to
`/tmp/zora_cex.log` / `/tmp/zora_dex.log`) and the spread detector
full-screen in a tmux session, then attaches you to it. The spread
detector's live output is what you're looking at; `Ctrl+B 1` / `Ctrl+B 2`
switch to the CEX/DEX log windows, `Ctrl+B 0` switches back, and
`Ctrl+B [` enters scroll mode.

To stop:

```
make stop
```

Sends SIGTERM to all three processes (so CEX/DEX clean up their shared
memory segments), waits up to 3s, force-kills anything still running,
and tears down the tmux session.

To check logs:

```
make logs
```

Prompts for CEX, DEX, or SPR (SPR just re-attaches to the live tmux
session) -- useful when you don't want to attach/detach from tmux for a
quick look.

Other targets: `make attach` (re-attach to an already-running session),
`make status` (PID/running check for all three), `make rebuild` (clean
+ build).
