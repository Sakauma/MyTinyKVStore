cmake_minimum_required(VERSION 3.15)
project(KVStore VERSION 1.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-g")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/target/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/target/lib)

find_package(Threads REQUIRED)

add_library(kvstore SHARED src/kvstore.cpp)
target_include_directories(kvstore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(kvstore PRIVATE Threads::Threads)

add_executable(kv_test tests/main.cpp)
target_link_libraries(kv_test PRIVATE kvstore Threads::Threads)

install(TARGETS kv_test DESTINATION target/bin)
install(TARGETS kvstore DESTINATION target/lib)
install(FILES include/kvstore.h DESTINATION include)
install(FILES include/kvstore.h DESTINATION include)

message(STATUS "配置完成！请运行 'make' 命令来构建项目。")