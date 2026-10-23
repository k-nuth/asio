#include <asio.hpp>
#include <asio/experimental/channel_select.hpp>
#include <array>
#include <barrier>
#include <chrono>
#include <iostream>
#include <thread>
using namespace asio::experimental;
using channel = select_channel<std::uint64_t>;
using clock_type = std::chrono::steady_clock;
void check(bool ok) { if (!ok) throw std::runtime_error("benchmark invariant"); }
asio::awaitable<void> producer(channel& c, std::uint64_t n) {
 auto ex=co_await asio::this_coro::executor;
 for(std::uint64_t i=0;i<n;++i) {
  auto [ec,r]=co_await make_select(send(c,i)).async_wait(ex,{}, {},asio::as_tuple(asio::use_awaitable));
  check(!ec && r.index==0);
 }
}
asio::awaitable<void> consumer(channel& a,channel& b,channel& c,std::uint64_t n,bool ready,bool mixed,std::array<std::uint64_t,3>& counts) {
 auto ex=co_await asio::this_coro::executor;
 for(std::uint64_t i=0;i<n;++i) {
  if(mixed) {
   auto [ec,r]=co_await make_select(receive(a),send(b,std::uint64_t{7}),receive(c)).async_wait(ex,{}, {},asio::as_tuple(asio::use_awaitable));
   check(!ec && r.index<3); ++counts[r.index];
   if(r.index==1) { asio::error_code e;check(b.try_receive(e)==7 && !e); }
   else if(r.index==0) {check(r.value<0>()==7);check(a.try_send(7));}
   else {check(r.value<2>()==7);check(c.try_send(7));}
  } else {
   auto [ec,r]=co_await make_select(receive(a),receive(b),receive(c)).async_wait(ex,{}, {},asio::as_tuple(asio::use_awaitable));
   check(!ec && r.index<3);
   auto value=r.index==0?*r.value<0>():r.index==1?*r.value<1>():*r.value<2>();
   check(value==(ready?7:counts[r.index])); ++counts[r.index];
   if(ready) {channel* channels[]={&a,&b,&c};check(channels[r.index]->try_send(7));}
  }
 }
}
int main(int argc,char**argv) {
 if(argc!=5)return 2;
 std::string mode=argv[1];std::uint64_t n=std::stoull(argv[2]);auto cap=std::stoul(argv[3]);int workers=std::stoi(argv[4]);
 bool ready=mode=="ready",mixed=mode=="mixed";check(ready||mixed||mode=="fanin");check(n>0&&workers>0);
 channel a(ready||mixed?1:cap),b(ready||mixed?1:cap),c(ready||mixed?1:cap);
 if(ready||mixed) {check(a.try_send(7));check(c.try_send(7));if(ready)check(b.try_send(7));}
 asio::io_context ctx;std::array<std::uint64_t,3> counts{};std::exception_ptr failure;
 // Every completion runs on this strand, including failure recording.
 auto owner=asio::make_strand(ctx);
 auto done=[&](std::exception_ptr e){if(e&&!failure)failure=e;};
 asio::co_spawn(ctx,consumer(a,b,c,ready||mixed?n:3*n,ready,mixed,counts),asio::bind_executor(owner,done));
 if(!ready&&!mixed)for(auto* ch:{&a,&b,&c})asio::co_spawn(ctx,producer(*ch,n),asio::bind_executor(owner,done));
 std::barrier gate(workers);
 std::vector<std::thread> runners;
 for(int i=1;i<workers;++i)runners.emplace_back([&]{gate.arrive_and_wait();ctx.run();});
 auto start=clock_type::now();
 gate.arrive_and_wait();ctx.run();for(auto& t:runners)t.join();
 auto ns=std::chrono::duration_cast<std::chrono::nanoseconds>(clock_type::now()-start).count();
 if(failure)std::rethrow_exception(failure);
 check(a.pending()+b.pending()+c.pending()==0);
 if(!ready&&!mixed)check(counts[0]==n&&counts[1]==n&&counts[2]==n&&a.buffered()+b.buffered()+c.buffered()==0);
 else check(a.buffered()+b.buffered()+c.buffered()==(mixed?2:3));
 std::cout<<ns<<','<<counts[0]<<','<<counts[1]<<','<<counts[2]<<'\n';
}
