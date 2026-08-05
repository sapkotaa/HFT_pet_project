cmake_minimum_required(VERSION 3.16)
project(hft_lob CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
option(NATIVE_ARCH "Build with -march=native" ON)
if(NATIVE_ARCH)
  add_compile_options(-march=native)
endif()

find_package(Threads REQUIRED)

add_executable(hft_lob src/main.cpp)
target_include_directories(hft_lob PRIVATE include)
target_link_libraries(hft_lob PRIVATE Threads::Threads)
