cmake_minimum_required(VERSION 3.20)
project(asio_consumer LANGUAGES CXX)
find_package(asio CONFIG REQUIRED)
add_executable(consumer consumer.cpp)
target_compile_features(consumer PRIVATE cxx_std_20)
target_link_libraries(consumer PRIVATE asio::asio)
# Ensure that the new API and ordinary Asio resolve from this package.
get_target_property(ASIO_INCLUDES asio::asio INTERFACE_INCLUDE_DIRECTORIES)
message(STATUS "Asio package include paths: ${ASIO_INCLUDES}")
