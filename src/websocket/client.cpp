#include "websocket/client.h"
#include "utils/logger.h"
#include "utils/timestamp.h"

#include <openssl/err.h>

#include <algorithm>
#include <chrono>
#include <thread>

namespace zora::websocket {

using zora::utils::LogLevel;

Client::Client(std::string host, std::string port, std::string path,
               MessageCallback on_message, ReconnectCallback on_reconnect)
    : host_(std::move(host))
    , port_(std::move(port))
    , path_(std::move(path))
    , on_message_(std::move(on_message))
    , on_reconnect_(std::move(on_reconnect))
    , ssl_ctx_(ssl::context::tlsv12_client)
    , ws_(std::make_unique<ws::stream<beast::ssl_stream<beast::tcp_stream>>>(ioc_, ssl_ctx_))
{
    ssl_ctx_.set_default_verify_paths();
    ssl_ctx_.set_verify_mode(ssl::verify_peer);
}

void Client::stop() {
    running_ = false;
    if (ws_) {
        beast::error_code ec;
        beast::get_lowest_layer(*ws_).close();
    }
    ioc_.stop();
}

void Client::reset_stream() {
    ioc_.restart();
    ws_ = std::make_unique<ws::stream<beast::ssl_stream<beast::tcp_stream>>>(ioc_, ssl_ctx_);
}

void Client::run() {
    running_ = true;
    int delay_ms = 1000;
    int attempt = 0;

    while (running_) {
        try {
            connect();
            delay_ms = 1000;
            attempt = 0;
        } catch (const std::exception& e) {
            if (!running_) break;

            ++attempt;
            if (on_reconnect_) on_reconnect_();

            char attempt_buf[8], reason_buf[64];
            std::snprintf(attempt_buf, sizeof(attempt_buf), "%d", attempt);
            std::snprintf(reason_buf, sizeof(reason_buf), "%s", e.what());

            zora::utils::log(LogLevel::Warn, {
                {"websocket", "reconnecting"}, {"attempt", attempt_buf},
                {"reason", reason_buf}, {"status", "RECONNECTING"}
            });

            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
            delay_ms = std::min(delay_ms * 2, 30000);

            reset_stream();
        }
    }
}

void Client::connect() {
    net::ip::tcp::resolver resolver(ioc_);
    auto endpoints = resolver.resolve(host_, port_);

    beast::get_lowest_layer(*ws_).connect(endpoints);

    if (!SSL_set_tlsext_host_name(ws_->next_layer().native_handle(), host_.c_str())) {
        throw beast::system_error(
            beast::error_code(static_cast<int>(::ERR_get_error()),
                               net::error::get_ssl_category()),
            "SSL SNI error");
    }
    ws_->next_layer().handshake(ssl::stream_base::client);
    ws_->handshake(host_, path_);

    zora::utils::log(LogLevel::Info, {{"websocket", "connected"}, {"status", "CONNECTED"}});
    running_ = true;

    do_read();
}

void Client::do_read() {
    while (running_) {
        try {
            buffer_.clear();
            ws_->read(buffer_);
        } catch (const std::exception&) {
            if (!running_) return; // clean shutdown, not a real disconnect
            throw;
        }

        uint64_t receive_ts_us = zora::utils::now_micros();
        auto data = buffer_.data();
        std::string_view msg(static_cast<const char*>(data.data()), data.size());
        on_message_(msg, receive_ts_us);
    }
}

} // namespace zora::websocket
