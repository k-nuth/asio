# First optimized C++ snapshot

`asio/experimental/channel_select.hpp` is byte-for-byte the header from
`6c107b9dc076532aaeaa5cdd74991afff93ded6a`, SHA256
`f299765376c3cecca2d5520dac5c7858c8628df3b07b583f39d73f4eba5a3612`.
The copyright and Boost Software License notice are retained.

`asio-v1` and `asio-v1-reuse` compile this overlay with the same benchmark source
and flags used by the current `asio` and `asio-reuse` engines. The reuse macro is
the only difference between each pair. This snapshot is benchmark-only and is
not installed in the Conan package. The earlier original implementation remains
available as `asio-baseline` under `../reference/`.
