#include "websocket/client.h"
#include "handlers/book_ticker_handler.h"
#include "ipc/shm_writer.h"
#include "utils/logger.h"
#include "zora_ipc.h"

#include <csignal>
#include <cstdio>
#include <cstdlib>

namespace {

constexpr const char* kHost = "stream.binance.com";
constexpr const char* kPort = "9443";
// Combined stream: required to subscribe to more than one ticker over
// a single WebSocket connection. Wraps every message as
// {"stream":"<name>","data":{...}} -- see book_ticker_handler.cpp for
// why that changes how messages are parsed vs. the old single-stream
// endpoint.
constexpr const char* kPath =
    "/stream?streams=solusdc@bookTicker/rayusdc@bookTicker/bonkusdc@bookTicker/zecusdc@bookTicker/uniusdc@bookTicker";

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

    zora::utils::print_banner("ZORA CEX INGESTOR", "Binance SOL/USDC + RAY/USDC + BONK/USDC + ZEC/USDC + UNI/USDC");

    install_signal_handlers();

    zora::ipc::ShmWriter sol_shm;
    if (!sol_shm.init(ZORA_SHM_CEX_NAME, "SOLUSDC")) {
        zora::utils::log(LogLevel::Error, {{"shm", "create_failed"}, {"pair", "SOL/USDC"}, {"status", "ERROR"}});
        return EXIT_FAILURE;
    }
    zora::utils::log(LogLevel::Info, {{"shm", "created"}, {"pair", "SOL/USDC"}, {"path", ZORA_SHM_CEX_NAME}});

    zora::ipc::ShmWriter ray_shm;
    if (!ray_shm.init(ZORA_SHM_CEX_RAY_NAME, "RAYUSDC")) {
        zora::utils::log(LogLevel::Error, {{"shm", "create_failed"}, {"pair", "RAY/USDC"}, {"status", "ERROR"}});
        return EXIT_FAILURE;
    }
    zora::utils::log(LogLevel::Info, {{"shm", "created"}, {"pair", "RAY/USDC"}, {"path", ZORA_SHM_CEX_RAY_NAME}});

    zora::ipc::ShmWriter bonk_shm;
    if (!bonk_shm.init(ZORA_SHM_CEX_BONK_NAME, "BONKUSDC")) {
        zora::utils::log(LogLevel::Error, {{"shm", "create_failed"}, {"pair", "BONK/USDC"}, {"status", "ERROR"}});
        return EXIT_FAILURE;
    }
    zora::utils::log(LogLevel::Info, {{"shm", "created"}, {"pair", "BONK/USDC"}, {"path", ZORA_SHM_CEX_BONK_NAME}});

    zora::ipc::ShmWriter zec_shm;
    if (!zec_shm.init(ZORA_SHM_CEX_ZEC_NAME, "ZECUSDC")) {
        zora::utils::log(LogLevel::Error, {{"shm", "create_failed"}, {"pair", "ZEC/USDC"}, {"status", "ERROR"}});
        return EXIT_FAILURE;
    }
    zora::utils::log(LogLevel::Info, {{"shm", "created"}, {"pair", "ZEC/USDC"}, {"path", ZORA_SHM_CEX_ZEC_NAME}});

    zora::ipc::ShmWriter uni_shm;
    if (!uni_shm.init(ZORA_SHM_CEX_UNI_NAME, "UNIUSDC")) {
        zora::utils::log(LogLevel::Error, {{"shm", "create_failed"}, {"pair", "UNI/USDC"}, {"status", "ERROR"}});
        return EXIT_FAILURE;
    }
    zora::utils::log(LogLevel::Info, {{"shm", "created"}, {"pair", "UNI/USDC"}, {"path", ZORA_SHM_CEX_UNI_NAME}});

    zora::handlers::BookTickerHandler handler({
        {"SOLUSDC", "SOL/USDC", &sol_shm},
        {"RAYUSDC", "RAY/USDC", &ray_shm},
        {"BONKUSDC", "BONK/USDC", &bonk_shm},
        {"ZECUSDC", "ZEC/USDC", &zec_shm},
        {"UNIUSDC", "UNI/USDC", &uni_shm},
    });

    zora::websocket::Client client(
        kHost, kPort, kPath,
        [&handler](std::string_view msg, uint64_t receive_ts_us) {
            handler.handle(msg, receive_ts_us);
        },
        [&sol_shm, &ray_shm, &bonk_shm, &zec_shm, &uni_shm]() {
            sol_shm.signal_gap();
            ray_shm.signal_gap();
            bonk_shm.signal_gap();
            zec_shm.signal_gap();
            uni_shm.signal_gap();
        }
    );

    g_client = &client;
    client.run();

    sol_shm.shutdown();
    ray_shm.shutdown();
    bonk_shm.shutdown();
    zec_shm.shutdown();
    uni_shm.shutdown();
    zora::utils::log(LogLevel::Info, {{"shutdown", "complete"}});
    return EXIT_SUCCESS;
}
