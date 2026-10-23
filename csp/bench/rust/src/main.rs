use std::{env,time::Instant};
fn crossbeam(mode:&str,n:u64,cap:usize) {
 use crossbeam_channel::{bounded,select};
 let ready=mode=="ready";let mixed=mode=="mixed";
 let cap=if ready||mixed{1}else{cap};
 let(a,ar)=bounded::<u64>(cap);let(b,br)=bounded::<u64>(cap);let(c,cr)=bounded::<u64>(cap);
 if ready||mixed{a.send(7).unwrap();c.send(7).unwrap();if ready{b.send(7).unwrap()}}
 let mut counts=[0u64;3];let mut handles=vec![];let gate=std::sync::Arc::new(std::sync::Barrier::new(if ready||mixed{1}else{4}));
 if !ready&&!mixed{for s in [&a,&b,&c]{let s=s.clone();let g=gate.clone();handles.push(std::thread::spawn(move||{g.wait();for i in 0..n{s.send(i).unwrap()}}))}}
 let start=Instant::now();gate.wait();
 for _ in 0..if ready||mixed{n}else{3*n}{
  let(k,v)=if mixed{select!{recv(ar)->v=>(0,v.unwrap()),send(b,7)->v=>{v.unwrap();(1,br.recv().unwrap())},recv(cr)->v=>(2,v.unwrap())}}else{select!{recv(ar)->v=>(0,v.unwrap()),recv(br)->v=>(1,v.unwrap()),recv(cr)->v=>(2,v.unwrap())}};
  assert_eq!(v,if ready||mixed{7}else{counts[k]});counts[k]+=1;
  if ready||(mixed&&k!=1){[&a,&b,&c][k].send(7).unwrap()}
 }
 for h in handles{h.join().unwrap()};let ns=start.elapsed().as_nanos();
 if ready||mixed{assert_eq!(ar.len()+br.len()+cr.len(),if mixed{2}else{3})}else{assert_eq!(counts,[n,n,n]);assert!(ar.is_empty()&&br.is_empty()&&cr.is_empty())}
 println!("{},{},{},{}",ns,counts[0],counts[1],counts[2]);
}
async fn tokio_run(mode:&str,n:u64,cap:usize){
 use tokio::sync::mpsc::channel;
 let ready=mode=="ready";let mixed=mode=="mixed";let cap=if ready||mixed{1}else{cap};
 let(a,mut ar)=channel::<u64>(cap);let(b,mut br)=channel::<u64>(cap);let(c,mut cr)=channel::<u64>(cap);
 if ready||mixed{a.try_send(7).unwrap();c.try_send(7).unwrap();if ready{b.try_send(7).unwrap()}}
 let mut handles=vec![];
 let gate=std::sync::Arc::new(tokio::sync::Barrier::new(if ready||mixed{1}else{4}));
 if !ready&&!mixed{for s in [&a,&b,&c]{let s=s.clone();let g=gate.clone();handles.push(tokio::spawn(async move{g.wait().await;for i in 0..n{s.send(i).await.unwrap()}}))}}
 let mut counts=[0u64;3];let start=Instant::now();gate.wait().await;
 for _ in 0..if ready||mixed{n}else{3*n}{
  // reserve keeps payload ownership if a send branch loses.
  let(k,v)=if mixed{tokio::select!{v=ar.recv()=>(0,v.unwrap()),p=b.reserve()=>{p.unwrap().send(7);(1,br.try_recv().unwrap())},v=cr.recv()=>(2,v.unwrap())}}else{tokio::select!{v=ar.recv()=>(0,v.unwrap()),v=br.recv()=>(1,v.unwrap()),v=cr.recv()=>(2,v.unwrap())}};
  assert_eq!(v,if ready||mixed{7}else{counts[k]});counts[k]+=1;
  if ready||(mixed&&k!=1){[&a,&b,&c][k].try_send(7).unwrap()}
 }
 for h in handles{h.await.unwrap()};let ns=start.elapsed().as_nanos();
 if ready||mixed{assert_eq!(ar.len()+br.len()+cr.len(),if mixed{2}else{3})}else{assert_eq!(counts,[n,n,n]);assert!(ar.is_empty()&&br.is_empty()&&cr.is_empty())}
 println!("{},{},{},{}",ns,counts[0],counts[1],counts[2]);
}
fn main(){let args:Vec<_>=env::args().collect();let engine=&args[1];let mode=&args[2];let n=args[3].parse().unwrap();let cap=args[4].parse().unwrap();let workers:usize=args[5].parse().unwrap();if engine=="crossbeam"{crossbeam(mode,n,cap)}else{let rt=if workers==1{tokio::runtime::Builder::new_current_thread().build().unwrap()}else{tokio::runtime::Builder::new_multi_thread().worker_threads(workers).build().unwrap()};let mode=mode.clone();rt.block_on(async move {tokio::spawn(async move {tokio_run(&mode,n,cap).await}).await.unwrap()});}}
