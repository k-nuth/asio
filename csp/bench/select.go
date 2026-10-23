package main
import("fmt";"os";"strconv";"runtime";"time";"sync")
func main(){
 mode:=os.Args[1];n,_:=strconv.Atoi(os.Args[2]);cap,_:=strconv.Atoi(os.Args[3]);workers,_:=strconv.Atoi(os.Args[4]);runtime.GOMAXPROCS(workers)
 ready,mixed:=mode=="ready",mode=="mixed";if ready||mixed{cap=1};a,b,c:=make(chan uint64,cap),make(chan uint64,cap),make(chan uint64,cap)
 if ready||mixed{a<-7;c<-7;if ready{b<-7}}
 var counts [3]uint64;var wg sync.WaitGroup
 // Start gate excludes setup while retaining scheduling and wake-up costs.
 gate:=make(chan struct{});total:=n
 if !ready&&!mixed{total=3*n;for _,ch:=range []chan uint64{a,b,c}{wg.Add(1);go func(ch chan uint64){defer wg.Done();<-gate;for i:=0;i<n;i++{ch<-uint64(i)}}(ch)}}
 start:=time.Now();close(gate)
 for i:=0;i<total;i++{var k int;var v uint64
  if mixed{select{case v=<-a:k=0;case b<-7:k=1;case v=<-c:k=2};if k==1{if <-b!=7{panic("send")}}else{if v!=7{panic("value")};if k==0{a<-7}else{c<-7}}}else{
   select{case v=<-a:k=0;case v=<-b:k=1;case v=<-c:k=2}
   expected:=counts[k];if ready{expected=7};if v!=expected{panic("FIFO")};if ready{[]chan uint64{a,b,c}[k]<-7}
  };counts[k]++
 };wg.Wait();ns:=time.Since(start).Nanoseconds()
 if !ready&&!mixed{for _,count:=range counts{if count!=uint64(n){panic("count")}};if len(a)+len(b)+len(c)!=0{panic("remaining")}}else{want:=3;if mixed{want=2};if len(a)+len(b)+len(c)!=want{panic("loser consumed")}}
 fmt.Printf("%d,%d,%d,%d\n",ns,counts[0],counts[1],counts[2])
}
