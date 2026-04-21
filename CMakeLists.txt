cmake_minimum_required(VERSION 3.15)
project(KVStore VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(KVSTORE_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(KVSTORE_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(KVSTORE_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(KVSTORE_ENABLE_COVERAGE "Enable coverage instrumentation" OFF)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/target/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/target/lib)

find_package(Threads REQUIRED)

function(kvstore_apply_sanitizers target_name)
    if (KVSTORE_ENABLE_TSAN AND (KVSTORE_ENABLE_ASAN OR KVSTORE_ENABLE_UBSAN))
        message(FATAL_ERROR "ThreadSanitizer cannot be combined with AddressSanitizer or UndefinedBehaviorSanitizer")
    endif()

    if (KVSTORE_ENABLE_ASAN)
        target_compile_options(${target_name} PRIVATE -fsanitize=address -fno-omit-frame-pointer)
        target_link_options(${target_name} PRIVATE -fsanitize=address -fno-omit-frame-pointer)
    endif()

    if (KVSTORE_ENABLE_UBSAN)
        target_compile_options(${target_name} PRIVATE -fsanitize=undefined -fno-omit-frame-pointer)
        target_link_options(${target_name} PRIVATE -fsanitize=undefined -fno-omit-frame-pointer)
    endif()

    if (KVSTORE_ENABLE_TSAN)
        target_compile_options(${target_name} PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(${target_name} PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
    endif()
endfunction()

function(kvstore_apply_coverage target_name)
    if (KVSTORE_ENABLE_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(${target_name} PRIVATE --coverage -O0 -g)
        target_link_options(${target_name} PRIVATE --coverage)
    endif()
endfunction()

add_library(kvstore SHARED
    src/kvstore.cpp
    src/internal/format.cpp
    src/internal/io.cpp
    src/internal/metrics_helpers.cpp
)
target_include_directories(kvstore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(kvstore PRIVATE Threads::Threads)
target_compile_options(kvstore PRIVATE -g -Wall -Wextra -Wpedantic)
kvstore_apply_sanitizers(kvstore)
kvstore_apply_coverage(kvstore)

add_executable(kv_test tests/main.cpp)
target_include_directories(kv_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(kv_test PRIVATE kvstore Threads::Threads)
target_compile_options(kv_test PRIVATE -g -Wall -Wextra -Wpedantic)
kvstore_apply_sanitizers(kv_test)
kvstore_apply_coverage(kv_test)

enable_testing()
add_test(NAME kv_test COMMAND kv_test)

install(TARGETS kv_test DESTINATION target/bin)
install(TARGETS kvstore DESTINATION target/lib)
install(FILES include/kvstore.h DESTINATION include)

message(STATUS "Configured KVStore. Build with 'cmake --build <build-dir>' and run tests from the build dir with 'ctest --output-on-failure'.")
