cmake_minimum_required(VERSION 3.10)
project(Research)

set(CMAKE_CXX_STANDARD 20)

link_libraries(pthread atomic)

add_executable(BQTest OpsTest.cpp BQ.cpp MSQ.cpp)
add_executable(BQTest2 kv_rw.cpp BQ.cpp)