cmake_minimum_required(VERSION 3.16)
project(zore_core)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
find_package(simdjson CONFIG REQUIRED)

add_executable(zore_core
        src/main.cpp
        src/websocket/client.cpp
        src/handlers/book_ticker_handler.cpp
        src/ipc/shm_writer.cpp
        include/websocket/client.h
        include/handlers/book_ticker_handler.h
        include/ipc/shm_writer.h
        include/utils/timestamp.h
        include/utils/logger.h
        shared/include/zora_ipc.h
)

target_include_directories(zore_core PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/shared/include
)

target_link_libraries(zore_core PRIVATE
        Boost::boost
        OpenSSL::SSL
        OpenSSL::Crypto
        Threads::Threads
        simdjson::simdjson
)

# glibc < 2.34 requires linking librt for shm_open/shm_unlink; folded into
# libc on newer glibc, and doesn't exist on macOS at all.
if(UNIX AND NOT APPLE)
    target_link_libraries(zore_core PRIVATE rt)
endif()
