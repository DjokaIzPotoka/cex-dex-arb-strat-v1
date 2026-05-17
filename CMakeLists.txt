cmake_minimum_required(VERSION 3.16)
project(zore_core)

set(CMAKE_CXX_STANDARD 20)
set(BOOST_ROOT "C:/vcpkg/installed/x64-mingw-dynamic")
set(BOOST_INCLUDEDIR "C:/vcpkg/installed/x64-mingw-dynamic/include")
set(BOOST_LIBRARYDIR "C:/vcpkg/installed/x64-mingw-dynamic/lib")
set(Boost_NO_SYSTEM_PATHS ON)
set(Boost_USE_STATIC_LIBS OFF)

set(OPENSSL_ROOT_DIR "C:/vcpkg/installed/x64-mingw-dynamic")
set(OPENSSL_INCLUDE_DIR "C:/vcpkg/installed/x64-mingw-dynamic/include")
set(OPENSSL_CRYPTO_LIBRARY "C:/vcpkg/installed/x64-mingw-dynamic/lib/libcrypto.dll.a")
set(OPENSSL_SSL_LIBRARY "C:/vcpkg/installed/x64-mingw-dynamic/lib/libssl.dll.a")

set(simdjson_DIR "C:/vcpkg/installed/x64-mingw-dynamic/share/simdjson")

find_package(Boost REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
find_package(simdjson CONFIG REQUIRED)

add_executable(zore_core src/main.cpp
        src/websocket_client.cpp
        include/websocket_client.h
        include/types.h
        include/decoder.h
        src/decoder.cpp)

target_include_directories(zore_core PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(zore_core PRIVATE
        Boost::system
        OpenSSL::SSL
        OpenSSL::Crypto
        Threads::Threads
        simdjson::simdjson
        ws2_32

)