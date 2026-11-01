#include <asio.hpp>
#include <asio/experimental/concurrent_channel.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <iostream>
#include <functional>
#include <cassert>

using channel = asio::experimental::concurrent_channel<void(std::error_code, int)>;
using namespace asio::experimental::awaitable_operators;

asio::awaitable<void> two_ready(channel& a, channel& b) {
    auto result = co_await (a.async_receive(asio::as_tuple(asio::use_awaitable)) ||
                           b.async_receive(asio::as_tuple(asio::use_awaitable)));
    int remaining = 0;
    a.try_receive([&](std::error_code, int){ ++remaining; });
    b.try_receive([&](std::error_code, int){ ++remaining; });
    assert(remaining == 0);
    std::cout << "two_ready winner=" << result.index()
              << " returned=1 remaining=" << remaining << " (sent=2)\n";
}

asio::awaitable<void> timed_receive(channel& a, int& winner, int& remaining) {
    asio::steady_timer timer(co_await asio::this_coro::executor);
    timer.expires_at(asio::steady_timer::clock_type::time_point::min());
    auto result = co_await (a.async_receive(asio::as_tuple(asio::use_awaitable)) ||
                           timer.async_wait(asio::as_tuple(asio::use_awaitable)));
    winner = result.index();
    a.try_receive([&](std::error_code, int){ ++remaining; });
}

void post_after(asio::io_context& ctx, channel& a, int depth) {
    asio::post(ctx, [&ctx, &a, depth] {
        if (depth == 0) a.try_send(std::error_code{}, 42);
        else post_after(ctx, a, depth - 1);
    });
}

int main() {
    std::cout << "ASIO_VERSION=" << ASIO_VERSION << '\n';
    {
        asio::io_context ctx;
        channel a(ctx, 1), b(ctx, 1);
        a.try_send(std::error_code{}, 1);
        b.try_send(std::error_code{}, 2);
        auto done = asio::co_spawn(ctx, two_ready(a, b), asio::use_future);
        ctx.run(); done.get();
    }
    for (int depth = 0; depth < 5; ++depth) {
        asio::io_context ctx;
        channel a(ctx, 1);
        int winner = -1, remaining = 0;
        auto done = asio::co_spawn(ctx, timed_receive(a, winner, remaining), asio::use_future);
        post_after(ctx, a, depth);
        ctx.run(); done.get();
        a.try_receive([&](std::error_code, int){ ++remaining; });
        if (depth == 1) assert(winner == 1 && remaining == 0);
        if (depth >= 2) assert(winner == 1 && remaining == 1);
        std::cout << "timer_receive depth=" << depth << " winner=" << winner
                  << " remaining=" << remaining << " (sent=1)\n";
    }
    for (bool cancel_first : {false, true}) {
        asio::io_context ctx;
        channel a(ctx, 1);
        a.try_send(std::error_code{}, 1);
        a.try_send(std::error_code{}, 2); // deliberately refused; buffer remains full
        int receive_done = 0, send_done = 0;
        channel empty(ctx, 1);
        empty.async_receive([&](std::error_code ec, int) {
            ++receive_done;
            std::cout << "close pending_receive=" << ec.message() << '\n';
        });
        a.async_send(std::error_code{}, 3, [&](std::error_code ec) {
            ++send_done;
            std::cout << "close pending_send=" << ec.message() << '\n';
        });
        if (cancel_first) { empty.cancel(); a.cancel(); }
        empty.close(); a.close();
        ctx.poll();
        assert(receive_done == 1);
        assert(send_done == (cancel_first ? 1 : 0));
        std::cout << "close cancel_first=" << cancel_first
                  << " receive_completed=" << receive_done
                  << " send_completed=" << send_done << '\n';
        // Preserve the observation above, then explicitly cancel and drain.
        a.cancel(); empty.cancel(); ctx.restart(); ctx.run();
        assert(send_done == 1);
    }
}
