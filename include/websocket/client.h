#pragma once
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace zora::websocket {

namespace net   = boost::asio;
namespace beast = boost::beast;
namespace ssl   = boost::asio::ssl;
namespace ws    = boost::beast::websocket;

// Called for every text message received. `msg` is only valid for the
// duration of the call (it points into the client's internal read buffer).
using MessageCallback = std::function<void(std::string_view msg, uint64_t receive_ts_us)>;

// Called once whenever a connection is lost and a reconnect is about to be
// attempted, before the backoff sleep. Optional.
using ReconnectCallback = std::function<void()>;

class Client {
public:
    Client(std::string host, std::string port, std::string path,
           MessageCallback on_message, ReconnectCallback on_reconnect = nullptr);

    // Blocking. Connects, reads until the connection drops, then
    // reconnects with exponential backoff. Returns once stop() is called.
    void run();
    void stop();

private:
    void connect();
    void do_read();
    void reset_stream();

    std::string host_;
    std::string port_;
    std::string path_;
    MessageCallback on_message_;
    ReconnectCallback on_reconnect_;
    std::atomic<bool> running_{false};
    net::io_context ioc_;
    ssl::context ssl_ctx_;
    std::unique_ptr<ws::stream<beast::ssl_stream<beast::tcp_stream>>> ws_;
    beast::flat_buffer buffer_;
};

} // namespace zora::websocket
