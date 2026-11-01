#include <asio/experimental/channel_select.hpp>
#include <asio.hpp>
#include <cassert>
#include <iostream>
#include <thread>
#include <barrier>
using namespace asio::experimental;
const auto expired = asio::steady_timer::time_point::min();
channel_select_options biased() { channel_select_options o; o.biased = true; return o; }

void ready_preserves_and_heterogeneous() {
  asio::io_context ctx; select_channel<int> a(1); select_channel<std::string> b(1);
  assert(a.try_send(10)); assert(b.try_send("twenty")); int calls = 0;
  make_select(receive(a), receive(b)).async_wait(ctx.get_executor(), biased(), {}, [&](auto ec, auto r) {
    assert(a.buffered() + b.buffered() == 1);
    assert(!ec && r.index == 0 && *r.template value<0>() == 10);
    assert(!r.template value<1>()); ++calls;
    assert(a.pending() == 0 && b.pending() == 0);
    assert(!b.try_send("wrong"));
    asio::error_code e; auto v = b.try_receive(e); assert(!e && v == "twenty");
  });
  ctx.run(); assert(calls == 1);
}
void mixed_send_receive_and_returned_payloads() {
  asio::io_context ctx; select_channel<int> a(1); select_channel<std::unique_ptr<int>> b(1);
  assert(a.try_send(8)); int calls = 0;
  make_select(receive(a), send(b, std::make_unique<int>(9))).async_wait(
    ctx.get_executor(), biased(), {}, [&](auto ec, auto r) {
      assert(!ec && r.index == 0 && *r.template value<0>() == 8);
      assert(**r.template value<1>() == 9); ++calls;
      assert(b.buffered() == 0 && b.pending() == 0);
    });
  ctx.run(); assert(calls == 1);
  ctx.restart();
  make_select(send(b, std::make_unique<int>(7)), receive(a)).async_wait(
    ctx.get_executor(), biased(), {}, [&](auto ec, auto r) {
      assert(!ec && r.index == 0 && !r.template value<0>()); ++calls;
      asio::error_code e; auto v = b.try_receive(e); assert(!e && **v == 7);
      assert(a.pending() == 0);
    });
  ctx.run(); assert(calls == 2);
}
void timer_boundary_and_pending_timer() {
  for (bool message_first : {true, false}) {
    asio::io_context ctx; select_channel<int> a(1); assert(a.try_send(42)); int calls = 0;
    if (message_first) make_select(receive(a), after(expired)).async_wait(
      ctx.get_executor(), biased(), {}, [&](auto ec, auto r) {
        assert(!ec && r.index == 0 && r.template value<0>() == 42); ++calls;
      });
    else make_select(after(expired), receive(a)).async_wait(ctx.get_executor(), biased(), {}, [&](auto ec, auto r) {
      assert(!ec && r.index == 0 && r.template value<0>()); ++calls;
      asio::error_code e; assert(a.try_receive(e) == 42 && !e);
    });
    ctx.run(); assert(calls == 1 && a.pending() == 0);
  }
  asio::io_context ctx; select_channel<int> a(0); int calls = 0;
  make_select(receive(a), after(asio::steady_timer::clock_type::now() + std::chrono::milliseconds(1))).async_wait(
    ctx.get_executor(), {}, {}, [&](auto ec, auto r) {
      assert(!ec && r.index == 1); assert(a.pending() == 0); ++calls;
    });
  ctx.run(); assert(calls == 1);
}
void cancellation_before_and_after_registration() {
  for (bool before : {false, true}) {
    asio::io_context ctx; select_channel<int> a(0), b(0); std::stop_source stop; int calls = 0;
    if (before) stop.request_stop();
    make_select(receive(a), send(b, 99)).async_wait(ctx.get_executor(), {}, stop.get_token(), [&](auto ec, auto r) {
      assert(ec == asio::error::operation_aborted && r.index == decltype(r)::npos);
      assert(r.template value<1>() == 99); assert(a.pending() == 0 && b.pending() == 0); ++calls;
    });
    if (!before) {
      // Only the start turn runs: admission has completed, no completion cleanup.
      assert(ctx.run_one() == 1); assert(a.pending() == 1 && b.pending() == 1);
      std::thread requester([&] { stop.request_stop(); }); requester.join();
    }
    ctx.run(); assert(calls == 1);
  }
}
void cancellation_during_handoff_and_slots() {
  asio::io_context ctx; select_channel<int> a(0), b(1); std::stop_source stop;
  asio::cancellation_signal signal; int calls = 0;
  make_select(receive(a), receive(b)).async_wait(ctx.get_executor(), {}, stop.get_token(),
    asio::bind_cancellation_slot(signal.slot(), [&](auto ec, auto r) {
      assert(!ec && r.index == 0 && r.template value<0>() == 15); ++calls;
      assert(a.pending() == 0 && b.pending() == 0);
    }));
  assert(ctx.run_one() == 1); assert(a.pending() == 1 && b.pending() == 1);
  assert(a.try_send(15)); // Claim and transfer, with completion not yet executed.
  stop.request_stop(); signal.emit(asio::cancellation_type::all);
  ctx.run(); assert(calls == 1);
  ctx.restart();
  make_select(receive(a)).async_wait(ctx.get_executor(), {}, {},
    asio::bind_cancellation_slot(signal.slot(), [&](auto ec, auto r) {
      assert(ec == asio::error::operation_aborted && !r.template value<0>()); ++calls;
      assert(a.pending() == 0);
    }));
  // Slot installed, but start turn not run yet. A ready payload must survive.
  signal.emit(asio::cancellation_type::all); ctx.run(); assert(calls == 2);
  ctx.restart(); select_channel<int> ready(1); assert(ready.try_send(64));
  make_select(receive(ready)).async_wait(ctx.get_executor(), {}, {},
    asio::bind_cancellation_slot(signal.slot(), [&](auto ec, auto r) {
      assert(ec == asio::error::operation_aborted && !r.template value<0>()); ++calls;
      asio::error_code e; assert(ready.try_receive(e) == 64 && !e);
    }));
  signal.emit(asio::cancellation_type::all); ctx.run(); assert(calls == 3);
}
void close_buffer_and_pending_producer() {
  asio::io_context ctx; select_channel<int> a(1), empty(0); int calls = 0;
  assert(a.try_send(1));
  make_select(send(a, 2)).async_wait(ctx.get_executor(), {}, {}, [&](auto ec, auto r) {
    assert(ec == asio::error::broken_pipe && r.template value<0>() == 2); ++calls;
    assert(a.pending() == 0);
  });
  assert(ctx.run_one() == 1); assert(a.pending() == 1);
  a.close(); assert(!a.try_send(3));
  make_select(receive(a)).async_wait(ctx.get_executor(), {}, {}, [&](auto ec, auto r) {
    assert(!ec && r.template value<0>() == 1); ++calls;
  });
  make_select(receive(empty)).async_wait(ctx.get_executor(), {}, {}, [&](auto ec, auto r) {
    assert(ec == asio::error::eof && !r.template value<0>()); ++calls;
  });
  empty.close(); ctx.run(); assert(calls == 3);
  asio::error_code ec; assert(!a.try_receive(ec) && ec == asio::error::eof);
}
void rendezvous_between_selects_and_no_self_match() {
  asio::io_context ctx; select_channel<int> a(0), b(0); int calls = 0;
  make_select(send(a, 7), receive(a)).async_wait(ctx.get_executor(), {}, {}, [&](auto ec, auto r) {
    assert(!ec && r.index == 0 && !r.template value<0>() && !r.template value<1>()); ++calls;
  });
  assert(ctx.run_one() == 1); assert(calls == 0 && a.pending() == 2);
  make_select(receive(a), send(b, 8)).async_wait(ctx.get_executor(), {}, {}, [&](auto ec, auto r) {
    assert(!ec && r.index == 0 && r.template value<0>() == 7 && r.template value<1>() == 8); ++calls;
    assert(a.pending() == 0 && b.pending() == 0);
  });
  ctx.run(); assert(calls == 2);
}
void bounds_default_and_fifo() {
  asio::io_context ctx; select_channel<int> a(0, 1); int calls = 0;
  std::stop_source stop;
  make_select(send(a, 1)).async_wait(ctx.get_executor(), {}, stop.get_token(), [&](auto ec, auto r) {
    assert(ec == asio::error::operation_aborted && r.template value<0>() == 1); ++calls;
  });
  assert(ctx.run_one() == 1); assert(a.pending() == 1);
  make_select(send(a, 2)).async_wait(ctx.get_executor(), {}, {}, [&](auto ec, auto r) {
    assert(ec == asio::error::no_buffer_space && r.template value<0>() == 2); ++calls;
    stop.request_stop();
  });
  ctx.run(); assert(calls == 2 && a.pending() == 0);
  ctx.restart(); channel_select_options opts; opts.nonblocking = true;
  make_select(receive(a), send(a, 9)).async_wait(ctx.get_executor(), opts, {}, [&](auto ec, auto r) {
    assert(ec == asio::error::would_block && r.template value<1>() == 9); ++calls;
  });
  ctx.run(); assert(calls == 3);
  ctx.restart(); select_channel<int> fifo(1); assert(fifo.try_send(0));
  for (int i = 1; i <= 4; ++i) make_select(send(fifo, i)).async_wait(ctx.get_executor(), {}, {},
    [&](auto ec, auto r) { assert(!ec && !r.template value<0>()); ++calls; });
  // Register all producers before reading; preserve original FIFO even on refill.
  for (int i = 0; i < 4; ++i) assert(ctx.run_one() == 1);
  for (int i = 0; i <= 4; ++i) { asio::error_code ec; assert(fifo.try_receive(ec) == i && !ec); }
  ctx.run(); assert(calls == 7 && fifo.pending() == 0);
}
struct tracked { int* n; explicit tracked(int& v) : n(&v) {} ~tracked() { ++*n; } };
void move_only_lifetime_and_context_teardown() {
  int destroyed = 0, calls = 0;
  {
    asio::io_context ctx; select_channel<std::unique_ptr<tracked>> a(0), b(0); std::stop_source stop;
    auto refused = std::make_unique<tracked>(destroyed);
    assert(!a.try_send(std::move(refused)) && refused); refused.reset();
    make_select(send(a, std::make_unique<tracked>(destroyed)), send(b, std::make_unique<tracked>(destroyed))).async_wait(
      ctx.get_executor(), {}, stop.get_token(), [&](auto ec, auto r) {
        assert(ec == asio::error::operation_aborted && r.template value<0>() && r.template value<1>()); ++calls;
      });
    assert(ctx.run_one() == 1); stop.request_stop(); ctx.run();
    assert(calls == 1 && a.pending() == 0 && b.pending() == 0 && destroyed == 3);
  }
  select_channel<int> outside(0);
  { asio::io_context ctx;
    make_select(receive(outside)).async_wait(ctx.get_executor(), {}, {}, [](auto, auto) { assert(false); });
    assert(ctx.run_one() == 1 && outside.pending() == 1);
  }
  assert(outside.pending() == 0); // Context teardown removes weak registrations.
}
void independent_executors_multithread() {
  asio::io_context left, right; select_channel<int> a(0), b(0); int sent = 0, got = 0;
  constexpr int count = 300; bool sends_valid = true, receives_valid = true;
  std::vector<int> seen(count, 0);
  auto send_owner = asio::make_strand(left), receive_owner = asio::make_strand(right);
  for (int i = 0; i < count; ++i) {
    make_select(send(a, i), send(b, i)).async_wait(left.get_executor(), {}, {}, asio::bind_executor(send_owner, [&, i](auto ec, auto r) {
      sends_valid = sends_valid && !ec && r.index < 2;
      if (r.index == 0) sends_valid = sends_valid && !r.template value<0>() && r.template value<1>() == i;
      if (r.index == 1) sends_valid = sends_valid && !r.template value<1>() && r.template value<0>() == i;
      ++sent;
    }));
    make_select(receive(a), receive(b)).async_wait(right.get_executor(), {}, {}, asio::bind_executor(receive_owner, [&](auto ec, auto r) {
      receives_valid = receives_valid && !ec && r.index < 2;
      auto& value = r.index == 0 ? r.template value<0>() : r.template value<1>();
      if (value && *value >= 0 && *value < count) ++seen[*value];
      else receives_valid = false;
      ++got;
    }));
  }
  std::barrier start{5};
  std::thread one([&] { start.arrive_and_wait(); left.run(); });
  std::thread two([&] { start.arrive_and_wait(); right.run(); });
  std::thread three([&] { start.arrive_and_wait(); left.run(); });
  std::thread four([&] { start.arrive_and_wait(); right.run(); });
  start.arrive_and_wait();
  one.join(); two.join(); three.join(); four.join();
  assert(sends_valid && receives_valid && sent == count && got == count);
  assert(std::all_of(seen.begin(), seen.end(), [](int n) { return n == 1; }));
  assert(a.pending() == 0 && b.pending() == 0 && a.buffered() == 0 && b.buffered() == 0);
}
void unbiased_smoke() {
  asio::io_context ctx; select_channel<int> a(1), b(1); int wins[2] = {};
  for (int i = 0; i < 256; ++i) {
    assert(a.try_send(1)); assert(b.try_send(2));
    channel_select_options options; options.random_seed = static_cast<std::uint32_t>(i);
    make_select(receive(a), receive(b)).async_wait(ctx.get_executor(), options, {}, [&](auto ec, auto r) {
      assert(!ec); ++wins[r.index]; asio::error_code e;
      if (r.index == 0) assert(b.try_receive(e) == 2 && !e);
      else assert(a.try_receive(e) == 1 && !e);
    });
    ctx.restart(); ctx.run();
  }
  // Reproducible seeds exercise both ready branches; no starvation-freedom claim.
  assert(wins[0] > 0 && wins[1] > 0);
}
struct delivery_gate {
  std::barrier<> entered{2}, release{2};
  bool armed = false;
};
struct gated_value {
  delivery_gate* gate;
  int value;
  gated_value(delivery_gate& g, int v) : gate(&g), value(v) {}
  gated_value(gated_value&& other) noexcept : gate(other.gate), value(other.value) {
    if (gate && gate->armed) {
      gate->armed = false;
      gate->entered.arrive_and_wait(); gate->release.arrive_and_wait();
    }
    other.gate = nullptr;
  }
};
void cancellation_inside_transfer() {
  asio::io_context ctx; select_channel<gated_value> channel(0);
  delivery_gate gate; gated_value payload(gate, 31); std::stop_source stop;
  int calls = 0; bool delivered_correctly = false;
  make_select(receive(channel)).async_wait(ctx.get_executor(), {}, stop.get_token(), [&](auto ec, auto r) {
    delivered_correctly = !ec && r.template value<0>() && r.template value<0>()->value == 31; ++calls;
  });
  assert(ctx.run_one() == 1);
  gate.armed = true;
  bool sent = false;
  std::thread producer([&] { sent = channel.try_send(std::move(payload)); });
  gate.entered.arrive_and_wait(); // Claim succeeded; move has not returned.
  stop.request_stop();
  auto progressed = ctx.run_one(); // Cancellation must not wait for this transfer.
  auto calls_during_transfer = calls;
  gate.release.arrive_and_wait(); producer.join();
  assert(progressed == 1 && calls_during_transfer == 0 && sent);
  ctx.run(); assert(calls == 1 && delivered_correctly && channel.pending() == 0);
}
void disabled_and_destroyed_channel() {
  asio::io_context ctx; select_channel<int> disabled(1); assert(disabled.try_send(1));
  int calls = 0;
  make_select(receive(disabled).when(false), after(expired)).async_wait(ctx.get_executor(), {}, {},
    [&](auto ec, auto r) { assert(!ec && r.index == 1 && disabled.buffered() == 1); ++calls; });
  ctx.run(); assert(calls == 1); ctx.restart();
  auto channel = std::make_unique<select_channel<int>>(0);
  make_select(receive(*channel)).async_wait(ctx.get_executor(), {}, {}, [&](auto ec, auto r) {
    assert(ec == asio::error::eof && !r.template value<0>()); ++calls;
  });
  assert(ctx.run_one() == 1); channel.reset(); ctx.run(); assert(calls == 2);
}
void send_selection_deadline_and_partial_admission() {
  asio::io_context ctx; select_channel<int> a(1), b(1); int calls = 0;
  make_select(send(a, 3), send(b, 4)).async_wait(ctx.get_executor(), biased(), {}, [&](auto ec, auto r) {
    assert(!ec && r.index == 0 && !r.template value<0>() && r.template value<1>() == 4);
    assert(a.buffered() == 1 && b.buffered() == 0); ++calls;
  });
  ctx.run(); assert(calls == 1); ctx.restart();
  auto deadline = biased(); deadline.deadline = expired;
  make_select(receive(a), send(b, 5)).async_wait(ctx.get_executor(), deadline, {}, [&](auto ec, auto r) {
    assert(ec == asio::error::timed_out && r.index == decltype(r)::npos);
    assert(a.buffered() == 1 && b.buffered() == 0 && r.template value<1>() == 5); ++calls;
  });
  ctx.run(); assert(calls == 2); ctx.restart();
  select_channel<int> full(0, 1), other(0); std::stop_source stop;
  make_select(send(full, 6)).async_wait(ctx.get_executor(), {}, stop.get_token(), [&](auto ec, auto r) {
    assert(ec == asio::error::operation_aborted && r.template value<0>() == 6); ++calls;
  });
  assert(ctx.run_one() == 1);
  make_select(receive(other), send(full, 7)).async_wait(ctx.get_executor(), biased(), {}, [&](auto ec, auto r) {
    assert(ec == asio::error::no_buffer_space && r.template value<1>() == 7);
    assert(other.pending() == 0 && full.pending() == 1); ++calls; stop.request_stop();
  });
  ctx.run(); assert(calls == 4 && full.pending() == 0);
}
void waiting_receiver_fifo() {
  asio::io_context ctx; select_channel<int> a(0); int calls = 0;
  for (int i = 0; i < 4; ++i) make_select(receive(a)).async_wait(ctx.get_executor(), {}, {},
    [&, i](auto ec, auto r) { assert(!ec && r.template value<0>() == i); ++calls; });
  for (int i = 0; i < 4; ++i) assert(ctx.run_one() == 1);
  for (int i = 0; i < 4; ++i) { auto value = i; assert(a.try_send(std::move(value))); }
  ctx.run(); assert(calls == 4 && a.pending() == 0);
}
void stopped_initiations_on_running_context() {
  asio::io_context ctx; auto work = asio::make_work_guard(ctx);
  auto callbacks = asio::make_strand(ctx); select_channel<int> channel(0);
  std::stop_source stop; stop.request_stop(); int calls = 0; bool valid = true;
  std::thread first([&] { ctx.run(); }), second([&] { ctx.run(); });
  for (int i = 0; i < 500; ++i) make_select(send(channel, i)).async_wait(ctx.get_executor(), {}, stop.get_token(),
    asio::bind_executor(callbacks, [&](auto ec, auto r) {
      valid = valid && ec == asio::error::operation_aborted && bool(r.template value<0>()); ++calls;
    }));
  work.reset(); first.join(); second.join();
  assert(valid && calls == 500 && channel.pending() == 0);
}
int main() {
  ready_preserves_and_heterogeneous(); mixed_send_receive_and_returned_payloads();
  timer_boundary_and_pending_timer(); cancellation_before_and_after_registration();
  cancellation_during_handoff_and_slots(); close_buffer_and_pending_producer();
  rendezvous_between_selects_and_no_self_match(); bounds_default_and_fifo();
  move_only_lifetime_and_context_teardown(); independent_executors_multithread(); unbiased_smoke();
  cancellation_inside_transfer(); disabled_and_destroyed_channel();
  send_selection_deadline_and_partial_admission(); waiting_receiver_fifo();
  stopped_initiations_on_running_context();
  std::cout << "independent heterogeneous send/receive select controls passed\n";
}
