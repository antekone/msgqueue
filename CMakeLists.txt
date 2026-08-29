cmake_minimum_required(VERSION 4.0)
project(msgqueue_test)

find_package(GTest)

add_executable(msgqueue_test main.cpp)

target_link_libraries(msgqueue_test PUBLIC GTest::gtest GTest::gtest_main)
target_compile_options(msgqueue_test PUBLIC -Wall)
target_compile_features(msgqueue_test PUBLIC cxx_std_23)
