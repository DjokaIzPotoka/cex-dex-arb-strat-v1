<<<<<<< HEAD
# Setup baze
docker-compose up -d
=======
# ZORA

CEX-DEX arbitrage bot for SOL/USDC. Two ingestors publish live prices
into POSIX shared memory for a downstream spread detector to consume:

- **CEX ingestor** (`src/`, C++): Binance `solusdc@bookTicker` via
  WebSocket → `/zora_cex_price`
- **DEX ingestor** (`ingestor-dex-solana/`): Raydium SOL/USDC pool via
  Solana RPC polling → `/zora_dex_price`

## Running both together

```
make build   # build both binaries (release)
make run     # start both in the background, with auto-restart on crash
make status  # check whether each process is running, and its PID
make logs    # follow combined [CEX]/[DEX]-prefixed output
make stop    # stop both cleanly (SIGTERM, so shared memory gets unlinked)
make clean   # remove build artifacts for both
```

`make run` builds either binary first if it isn't already built. If
either ingestor crashes, it's automatically restarted (up to 5 times);
after that, both are stopped.
