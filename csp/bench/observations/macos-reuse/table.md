| Engine | Scenario | Capacity | Workers | Median ns/message | Min–max |
|---|---|---:|---:|---:|---:|
| asio | ready | 1 | 1 | 75.0 | 74.6–76.0 |
| asio | mixed | 1 | 1 | 75.3 | 73.9–76.2 |
| asio | fanin | 0 | 1 | 535.2 | 530.7–538.2 |
| asio | fanin | 0 | 4 | 2312.3 | 2254.0–2325.2 |
| asio | fanin | 1 | 1 | 361.3 | 359.5–364.9 |
| asio | fanin | 1 | 4 | 1159.5 | 1085.2–1175.6 |
| asio | fanin | 64 | 1 | 117.5 | 116.9–119.7 |
| asio | fanin | 64 | 4 | 423.3 | 413.4–431.9 |
| asio-reuse | ready | 1 | 1 | 75.2 | 74.3–78.1 |
| asio-reuse | mixed | 1 | 1 | 76.8 | 73.7–80.0 |
| asio-reuse | fanin | 0 | 1 | 541.1 | 534.0–560.1 |
| asio-reuse | fanin | 0 | 4 | 2296.6 | 2272.8–2308.5 |
| asio-reuse | fanin | 1 | 1 | 361.4 | 360.5–367.5 |
| asio-reuse | fanin | 1 | 4 | 1145.0 | 1131.3–1170.0 |
| asio-reuse | fanin | 64 | 1 | 118.9 | 118.6–120.9 |
| asio-reuse | fanin | 64 | 4 | 396.1 | 379.4–412.7 |
