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

add_executable(feed_publisher src/feed_publisher.cpp)
target_include_directories(feed_publisher PRIVATE include)

add_executable(engine_subscriber src/engine_subscriber.cpp)
target_include_directories(engine_subscriber PRIVATE include)

add_executable(supervisor src/supervisor.cpp)

add_executable(test_frame_reader tests/test_frame_reader.cpp)
target_include_directories(test_frame_reader PRIVATE include)
target_compile_features(test_frame_reader PRIVATE cxx_std_20)
target_compile_options(test_frame_reader PRIVATE -UNDEBUG)   # <-- here


enable_testing()
add_test(NAME frame_reader_tests COMMAND test_frame_reader)

add_executable(test_session_state tests/test_session_state.cpp
  src/gateway_main.cpp)
target_include_directories(test_session_state PRIVATE include)
target_compile_features(test_session_state PRIVATE cxx_std_20)
target_compile_options(test_session_state PRIVATE -UNDEBUG)
target_link_libraries(test_session_state PRIVATE Threads::Threads)

add_test(NAME session_state_tests COMMAND test_session_state)

add_executable(gateway src/gateway_main.cpp)
target_include_directories(gateway PRIVATE include)
target_link_libraries(gateway PRIVATE Threads::Threads)







