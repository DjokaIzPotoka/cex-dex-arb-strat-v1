#ifndef ZORE_CORE_WEBSOCKET_CLIENT_H
#define ZORE_CORE_WEBSOCKET_CLIENT_H

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <string>
#include <functional>
#include <atomic>
#include <memory>

namespace net   = boost::asio;
namespace beast = boost::beast;
namespace ssl   = boost::asio::ssl;
namespace ws    = boost::beast::websocket;

using MessageCallback = std::function<void(const std::string&)>;

class WebSocketClient {
public:

    WebSocketClient(std::string host, std::string port,
                   std::string path, MessageCallback on_message);

    void run();
    void stop();

private:
    void connect();
    void do_read();

    std::string host_;
    std::string port_;
    std::string path_;
    MessageCallback on_message_;
    std::atomic<bool> running_{false};
    net::io_context ioc_;
    ssl::context ssl_ctx_;
    std::unique_ptr<ws::stream<beast::ssl_stream<beast::tcp_stream>>> ws_;
    beast::flat_buffer buffer_;

};

#endif // ZORE_CORE_WEBSOCKET_CLIENT_H