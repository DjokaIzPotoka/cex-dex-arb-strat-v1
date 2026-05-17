#include "websocket_client.h"
#include <openssl/err.h>
#include <algorithm>
#include <iostream>
#include <thread>
#include <chrono>

WebSocketClient::WebSocketClient(
    std::string host,
    std::string port,
    std::string path,
    MessageCallback on_message)
    : host_(std::move(host))
    , port_(std::move(port))
    , path_(std::move(path))
    , on_message_(std::move(on_message))
    , ssl_ctx_(ssl::context::tlsv12_client)
    , ws_(std::make_unique<ws::stream<beast::ssl_stream<beast::tcp_stream>>>(
          ioc_, ssl_ctx_))
{
    ssl_ctx_.set_default_verify_paths();
    ssl_ctx_.set_verify_mode(ssl::verify_peer);
}

void WebSocketClient::stop() {
    running_ = false;
    if (ws_) {
        beast::get_lowest_layer(*ws_).close();
    }
    ioc_.stop();
}

void WebSocketClient::run() {
    running_ = true;
    int delay_ms = 1000;

    while (running_) {
        try {
            connect();
            delay_ms = 1000; // reset backoff nakon uspesne konekcije
        } catch (const std::exception& e) {
            if (!running_) break;
            std::cerr << "[WS] Greska: " << e.what()
                      << " — reconnect za " << delay_ms << "ms\n";

            std::this_thread::sleep_for(
                std::chrono::milliseconds(delay_ms));

            delay_ms = std::min(delay_ms * 2, 30000);

            ioc_.restart();
            ws_ = std::make_unique<ws::stream<beast::ssl_stream<beast::tcp_stream>>>(ioc_, ssl_ctx_);
        }
    }
}

void WebSocketClient::connect() {
    net::ip::tcp::resolver resolver(ioc_);
    auto endpoints = resolver.resolve(host_, port_);

    beast::get_lowest_layer(*ws_).connect(endpoints);

    if (!SSL_set_tlsext_host_name(
            ws_->next_layer().native_handle(), host_.c_str())) {
        throw beast::system_error(
            beast::error_code(static_cast<int>(::ERR_get_error()),
            net::error::get_ssl_category()), "SSL SNI greska");
            }
    ws_->next_layer().handshake(ssl::stream_base::client);

    ws_->handshake(host_, path_);

    std::cout << "[WS] Povezan na " << host_ << path_ << "\n";
    running_ = true;

    do_read();
}

void WebSocketClient::do_read() {
    while (running_) {
        try {
            buffer_.clear();
            ws_->read(buffer_);

            std::string msg = beast::buffers_to_string(buffer_.data());
            on_message_(msg);

        } catch (const std::exception& e) {
            if (!running_) break;
            throw;
        }
    }
}