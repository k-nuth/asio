// Copyright (c) 2026 Knuth contributors
// Distributed under the Boost Software License, Version 1.0.
#include "timer_probe.hpp"
#include <asio/experimental/channel_select.hpp>
#include <cassert>
#include <iostream>

int main() {
  using namespace asio::experimental;
  asio::io_context context;
  select_channel<int> input(1);
  bool completed = false;
  make_select(receive(input), after(asio::steady_timer::clock_type::now() +
      std::chrono::milliseconds(1))).async_wait(context.get_executor(), {}, {},
      [&](auto ec, auto result) {
        assert(!ec && result.index == 1);
        completed = true;
      });
  context.run();
  assert(completed && input.pending() == 0);
#if defined(ASIO_CHANNEL_SELECT_TIMER_TYPE)
  assert(timer_probe::waits == 1);
#else
  assert(timer_probe::waits == 0);
#endif

  context.restart();
  std::stop_source stop;
  completed = false;
  make_select(receive(input), after(asio::steady_timer::clock_type::now() +
      std::chrono::hours(1))).async_wait(context.get_executor(), {}, stop.get_token(),
      [&](auto ec, auto) {
        assert(ec == asio::error::operation_aborted);
        completed = true;
      });
  context.poll();
  assert(!completed && input.pending() == 1);
  stop.request_stop();
  context.run();
  assert(completed && input.pending() == 0);
#if defined(ASIO_CHANNEL_SELECT_TIMER_TYPE)
  assert(timer_probe::waits == 2 && timer_probe::cancellations >= 2);
#endif
  std::cout << "timer backend expiry and cancellation passed\n";
}
