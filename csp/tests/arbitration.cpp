#include <asio.hpp>
#include <asio/experimental/channel_select.hpp>
#include <algorithm>
#include <array>
#include <cassert>
#include <random>
using namespace asio::experimental;

// The explicit seed preserves the previous full-permutation contract on each
// standard library. Check both the coroutine probe and deferred-token path.
asio::awaitable<void> probe(std::array<select_channel<int>, 5>& channels,
    channel_select_options options, std::size_t expected) {
  auto ex = co_await asio::this_coro::executor;
  auto completion = co_await make_select(receive(channels[0]), receive(channels[1]),
      receive(channels[2]), receive(channels[3]), receive(channels[4]))
      .async_wait(ex, options, {}, asio::as_tuple(asio::use_awaitable));
  auto& [ec, result] = completion;
  assert(!ec && result.index == expected);
}
int main() {
  for (unsigned seed = 0; seed < 128; ++seed) {
    for (bool coroutine : {false, true}) {
      asio::io_context context;
      std::array<select_channel<int>, 5> channels{
        select_channel<int>(1), select_channel<int>(1), select_channel<int>(1),
        select_channel<int>(1), select_channel<int>(1)};
      for (int i = 0; i < 5; ++i) assert(channels[i].try_send(int(i)));
      channel_select_options options;
      options.random_seed = seed;
      options.biased = (seed % 8 == 0);
      std::array<std::size_t, 5> order{0, 1, 2, 3, 4};
      if (!options.biased) {
        std::mt19937 random(seed);
        std::shuffle(order.begin(), order.end(), random);
      }
      bool completed = false;
      if (coroutine) {
        asio::co_spawn(context, probe(channels, options, order[0]),
          [&](std::exception_ptr error) { if (error) std::rethrow_exception(error); completed = true; });
      } else {
        make_select(receive(channels[0]), receive(channels[1]), receive(channels[2]),
            receive(channels[3]), receive(channels[4]))
          .async_wait(context.get_executor(), options, {}, [&](auto ec, auto result) {
            assert(!ec && result.index == order[0]); completed = true;
          });
      }
      context.run();
      assert(completed);
      for (std::size_t i = 0; i < channels.size(); ++i) {
        assert(channels[i].pending() == 0);
        assert(channels[i].buffered() == (i == order[0] ? 0 : 1));
        asio::error_code ec;
        auto value = channels[i].try_receive(ec);
        if (i == order[0]) assert(!value && ec == asio::error::would_block);
        else assert(value && *value == static_cast<int>(i) && !ec);
      }
    }
  }
}
