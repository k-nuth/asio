#include <asio/experimental/channel_select.hpp>
#include <asio/io_context.hpp>
#include <iostream>
int main() {
  asio::io_context context;
  asio::experimental::select_channel<int> channel(1);
  if (!channel.try_send(42)) return 3;
  bool completed = false;
  asio::experimental::make_select(asio::experimental::receive(channel)).async_wait(
    context.get_executor(), {}, {}, [&](auto ec, auto result) {
      completed = !ec && result.template value<0>() == 42 && channel.pending() == 0;
    });
  try { context.run(); }
  catch (const asio::execution::bad_executor& error) { std::cout << error.what() << '\n'; return 2; }
  if (!completed) return 4;
  std::cout << "completion executor retained and callback delivered\n";
}
