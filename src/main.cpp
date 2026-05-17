#include "websocket_client.h"
#include "decoder.h"
#include "types.h"
#include <iostream>
#include <atomic>
#include <chrono>
#include <windows.h>

// ── Konfiguracija ──────────────────────────────
static const std::string HOST   = "fstream.binance.com";
static const std::string PORT   = "443";
static const std::string STREAM = "btcusdt@trade";
// ───────────────────────────────────────────────

static std::atomic<bool>     g_running{true};
static std::atomic<uint64_t> g_total_trades{0};
static WebSocketClient*      g_client = nullptr;
static auto                  g_start  = std::chrono::steady_clock::now();

BOOL WINAPI console_handler(DWORD signal) {
    if (signal == CTRL_C_EVENT) {
        auto now     = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>
                       (now - g_start).count();

        std::cout << "\n──────────────────────────────\n";
        std::cout << "[STATS] Ukupno:  " << g_total_trades << "\n";
        std::cout << "[STATS] Vreme rada:     " << elapsed << "s\n";
        if (elapsed > 0)
            std::cout << "[STATS] Prosek/sec:     "
                      << g_total_trades / elapsed << "\n";
        std::cout << "──────────────────────────────\n";

        g_running = false;
        if (g_client) g_client->stop();
        return TRUE;
    }
    return FALSE;
}

int main() {
    SetConsoleCtrlHandler(console_handler, TRUE);

    std::cout << "[Main] Starting...\n";

    TradeDecoder decoder;
    std::string path = "/ws/" + STREAM;

    WebSocketClient client(
        HOST, PORT, path,
        [&](const std::string& msg) {
            auto trade = decoder.decode(msg);
            if (!trade.has_value()) return;

            g_total_trades++;

            int64_t latency_ms = (trade->receive_ts - trade->timestamp) / 1'000'000;

            std::cout << "[BINANCE] "
                      << trade->symbol
                      << " | Price: "  << trade->price
                      << " | Side: "   << trade->side
                      << " | Qty: "    << trade->amount
                      << " | Latency: " << latency_ms << "ms"
                      << "\n";
        }
    );

    g_client = &client;
    client.run();

    return 0;
}