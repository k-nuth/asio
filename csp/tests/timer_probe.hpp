// Copyright (c) 2026 Knuth contributors
// Distributed under the Boost Software License, Version 1.0.
#pragma once
#include <asio/steady_timer.hpp>
#include <utility>

namespace timer_probe {
inline unsigned waits = 0;
inline unsigned cancellations = 0;
class timer {
  asio::steady_timer timer_;
public:
  using clock_type = asio::steady_timer::clock_type;
  using time_point = asio::steady_timer::time_point;
  explicit timer(asio::any_io_executor executor) : timer_(std::move(executor)) {}
  auto expires_at(time_point expiry) { return timer_.expires_at(expiry); }
  template<class Token> auto async_wait(Token&& token) {
    ++waits;
    return timer_.async_wait(std::forward<Token>(token));
  }
  auto cancel() { ++cancellations; return timer_.cancel(); }
};
}
