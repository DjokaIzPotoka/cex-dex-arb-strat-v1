#include "websocket/client.h"
#include "handlers/book_ticker_handler.h"
#include "ipc/shm_writer.h"
#include "utils/logger.h"
#include "zora_ipc.h"

#include <csignal>
#include <cstdio>
#include <cstdlib>

namespace {

constexpr const char* kHost   = "stream.binance.com";
constexpr const char* kPort   = "9443";
constexpr const char* kPath   = "/ws/solusdc@bookTicker";
constexpr const char* kSymbol = "SOLUSDC";

zora::websocket::Client* g_client = nullptr;

extern "C" void handle_signal(int) {
    if (g_client) g_client->stop();
}

void install_signal_handlers() {
    struct sigaction sa{};
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0; // no SA_RESTART: lets a blocked read() unblock on signal
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
}

} // namespace

int main() {
    using zora::utils::LogLevel;

    // C's stdio switches from line-buffered to fully block-buffered
    // (4-8KB) the moment stdout isn't a TTY -- which is always true
    // when this runs under scripts/start.sh's log-prefixing pipe.
    // Without this, log lines can sit unflushed for a long time at low
    // tick rates, breaking the "visible in real time" point of that
    // pipe. Line-buffering costs a flush per log line, which is
    // already priced in: logging was already the dominant cost next to
    // the <10us compute budget (see book_ticker_handler.cpp).
    setvbuf(stdout, nullptr, _IOLBF, 0);

    zora::utils::print_banner("ZORA CEX INGESTOR", "Binance SOL/USDC  bookTicker");

    install_signal_handlers();

    zora::ipc::ShmWriter shm;
    if (!shm.init(kSymbol)) {
        zora::utils::log(LogLevel::Error, {{"shm", "create_failed"}, {"status", "ERROR"}});
        return EXIT_FAILURE;
    }
    zora::utils::log(LogLevel::Info, {{"shm", "created"}, {"path", ZORA_SHM_CEX_NAME}});

    zora::handlers::BookTickerHandler handler(shm);

    zora::websocket::Client client(
        kHost, kPort, kPath,
        [&handler](std::string_view msg, uint64_t receive_ts_us) {
            handler.handle(msg, receive_ts_us);
        },
        [&shm]() {
            shm.signal_gap();
        }
    );

    g_client = &client;
    client.run();

    shm.shutdown();
    zora::utils::log(LogLevel::Info, {{"shutdown", "complete"}});
    return EXIT_SUCCESS;
}
