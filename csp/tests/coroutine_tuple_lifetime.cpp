// Standard-C++ reproduction: no Asio headers, channels, or runtime.
// On the measured GCC 16.1 -O1 configuration, the first direct structured
// binding survives incorrectly across a later suspension. Named tuple storage
// retains exactly the same ownership and observations without explicit cleanup.
#include <coroutine>
#include <exception>
#include <memory>
#include <optional>
#include <tuple>
#include <iostream>
#include <utility>
#include <system_error>
template<class T> struct task {
 struct promise_type {
  std::optional<T> value;
  std::coroutine_handle<> continuation = std::noop_coroutine();
  task get_return_object(){return task{std::coroutine_handle<promise_type>::from_promise(*this)};}
  std::suspend_always initial_suspend() noexcept{return {};}
  auto final_suspend() noexcept {
   struct final_awaiter { bool await_ready() noexcept{return false;}
    std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept{return h.promise().continuation;}
    void await_resume() noexcept{} };
   return final_awaiter{};
  }
  template<class U> void return_value(U&&v){value.emplace(std::forward<U>(v));}
  void unhandled_exception(){std::terminate();}
 };
 std::coroutine_handle<promise_type> h;
 task(std::coroutine_handle<promise_type> p):h(p){}
 task(task&&o):h(std::exchange(o.h,{})){}
 ~task(){if(h)h.destroy();}
 bool await_ready() noexcept{return false;}
 std::coroutine_handle<> await_suspend(std::coroutine_handle<> parent) noexcept{h.promise().continuation=parent;return h;}
 T await_resume(){return std::move(*h.promise().value);}
};
task<std::tuple<std::error_code,std::shared_ptr<int>>> value(std::shared_ptr<int> p){co_return std::make_tuple(std::error_code{},std::move(p));}
task<int> scenario(std::weak_ptr<int>& first,std::weak_ptr<int>& second){
 auto p=std::make_shared<int>(1);first=p;
#ifdef NAMED_TUPLE_STORAGE
 auto first_result=co_await value(std::move(p)); auto& [ec,a]=first_result;
#else
 auto [ec,a]=co_await value(std::move(p));
#endif
 auto q=std::make_shared<int>(2);second=q;
#ifdef NAMED_TUPLE_STORAGE
 auto second_result=co_await value(std::move(q)); auto& [ec2,b]=second_result;
#else
 auto [ec2,b]=co_await value(std::move(q));
#endif
 if(ec||ec2||*a!=1||*b!=2)std::terminate();
 co_return 0;
}
int main(){std::weak_ptr<int>a,b;{auto done=scenario(a,b);done.h.resume();}std::cout<<"standard owners="<<a.use_count()<<","<<b.use_count()<<std::endl;return a.expired()&&b.expired()?0:2;}
