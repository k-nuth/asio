| Engine | Scenario | Capacity | Workers | Median ns/message | Min–max |
|---|---|---:|---:|---:|---:|
| asio | ready | 1 | 1 | 78.6 | 77.9–80.4 |
| asio | mixed | 1 | 1 | 79.5 | 77.9–95.7 |
| asio | fanin | 0 | 1 | 644.7 | 639.4–654.2 |
| asio | fanin | 0 | 4 | 978.6 | 972.7–1193.1 |
| asio | fanin | 1 | 1 | 435.7 | 433.7–438.3 |
| asio | fanin | 1 | 4 | 448.9 | 436.9–455.1 |
| asio | fanin | 64 | 1 | 139.7 | 139.0–141.8 |
| asio | fanin | 64 | 4 | 395.7 | 329.0–486.7 |
| asio-reuse | ready | 1 | 1 | 82.6 | 81.7–101.4 |
| asio-reuse | mixed | 1 | 1 | 79.7 | 78.8–96.7 |
| asio-reuse | fanin | 0 | 1 | 626.8 | 622.6–631.4 |
| asio-reuse | fanin | 0 | 4 | 979.7 | 973.7–1194.4 |
| asio-reuse | fanin | 1 | 1 | 422.2 | 418.8–427.4 |
| asio-reuse | fanin | 1 | 4 | 415.0 | 413.7–513.3 |
| asio-reuse | fanin | 64 | 1 | 138.0 | 136.6–139.1 |
| asio-reuse | fanin | 64 | 4 | 301.4 | 295.4–307.5 |
