from conan import ConanFile
from conan.tools.files import copy
import os


class AsioCsp(ConanFile):
    name = "asio"
    version = "1.38.3-kth.1"
    license = "BSL-1.0"
    url = "https://github.com/k-nuth/asio"
    description = "Standalone Asio with experimental independent heterogeneous channel selection"
    package_type = "header-library"
    settings = "os", "arch", "compiler", "build_type"
    exports_sources = "include/*", "LICENSE_1_0.txt", "COPYING", "csp/README.md"

    def package_id(self):
        self.info.clear()

    def package(self):
        copy(self, "*.hpp", os.path.join(self.source_folder, "include"),
             os.path.join(self.package_folder, "include"))
        copy(self, "*.ipp", os.path.join(self.source_folder, "include"),
             os.path.join(self.package_folder, "include"))
        for name in ("LICENSE_1_0.txt", "COPYING"):
            copy(self, name, self.source_folder, os.path.join(self.package_folder, "licenses"))
        copy(self, "README.md", os.path.join(self.source_folder, "csp"),
             os.path.join(self.package_folder, "res", "csp"))

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "asio")
        self.cpp_info.set_property("cmake_target_name", "asio::asio")
        self.cpp_info.defines = ["ASIO_STANDALONE"]
        self.cpp_info.bindirs = []
        self.cpp_info.libdirs = []
        if self.settings.os == "Windows":
            self.cpp_info.system_libs = ["ws2_32", "mswsock"]
        elif self.settings.os in ("Linux", "FreeBSD"):
            self.cpp_info.system_libs = ["pthread"]
