# Frozen C++ baseline

`asio/experimental/channel_select.hpp` is byte-for-byte the header from
`035b2c7850e26f8d8ecd826899b76ae9bc9db9ae` in k-nuth/asio, before performance
changes. SHA256: `1a553194765e1e73513f4584442fd03da12b4d55a932d3bd00c08be70d26e27b`.
Its Boost Software License notice is retained; the license is at repository root.

`build.py` puts this overlay before the normal include directory to build
`asio-baseline-bench`; `asio-bench` builds the current implementation. Both use
exactly the same benchmark source and compiler flags. The overlay is restricted
to benchmarks and is not installed or included by the library or Conan package.
