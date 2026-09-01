cmake_minimum_required(VERSION 3.15)
project(KVStore VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(KVSTORE_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(KVSTORE_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(KVSTORE_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(KVSTORE_ENABLE_COVERAGE "Enable coverage instrumentation" OFF)

if (KVSTORE_ENABLE_TSAN)
    include(CheckCXXSourceCompiles)
    set(_kvstore_saved_required_flags "${CMAKE_REQUIRED_FLAGS}")
    set(_kvstore_saved_required_link_options "${CMAKE_REQUIRED_LINK_OPTIONS}")
    set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -fsanitize=thread")
    set(CMAKE_REQUIRED_LINK_OPTIONS "${CMAKE_REQUIRED_LINK_OPTIONS};-fsanitize=thread")
    unset(KVSTORE_TSAN_RUNTIME_AVAILABLE CACHE)
    check_cxx_source_compiles(
        "#include <thread>\nint main() { std::thread worker([] {}); worker.join(); return 0; }"
        KVSTORE_TSAN_RUNTIME_AVAILABLE)
    set(CMAKE_REQUIRED_FLAGS "${_kvstore_saved_required_flags}")
    set(CMAKE_REQUIRED_LINK_OPTIONS "${_kvstore_saved_required_link_options}")
    if (NOT KVSTORE_TSAN_RUNTIME_AVAILABLE)
        message(FATAL_ERROR "KVSTORE_ENABLE_TSAN=ON requires a linkable ThreadSanitizer runtime")
    endif()
endif()

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
        target_compile_definitions(${target_name} PRIVATE KVSTORE_TSAN_INSTRUMENTED=1)
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
    src/internal/io.cpp
    src/internal/key_codec.cpp
    src/internal/metrics_helpers.cpp
    src/internal/observability.cpp
    src/internal/writer_policy.cpp
    src/internal/storage_engine.cpp
    src/internal/storage_format.cpp
    src/internal/value_cache.cpp
)
target_include_directories(kvstore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(kvstore PRIVATE Threads::Threads)
target_compile_options(kvstore PRIVATE -Wall -Wextra -Wpedantic $<$<CONFIG:Debug>:-g>)
kvstore_apply_sanitizers(kvstore)
kvstore_apply_coverage(kvstore)

add_executable(kv_test
    tests/common/benchmark_analysis.cpp
    tests/common/benchmark_cli.cpp
    tests/common/benchmark_runtime.cpp
    tests/common/benchmark_shared.cpp
    tests/common/format_analysis.cpp
    tests/common/format_cli.cpp
    tests/common/qualification_benchmark.cpp
    tests/common/qualification_soak.cpp
    tests/common/runtime_profiles.cpp
    tests/common/runtime_stress.cpp
    tests/common/test_driver.cpp
    tests/common/test_support.cpp
    tests/integration/all_tests.cpp
    tests/integration/basic_kv_tests.cpp
    tests/integration/benchmark_trend_tests.cpp
    tests/integration/durability_smoke_tests.cpp
    tests/integration/json_cli_tests.cpp
    tests/integration/metrics_controller_tests.cpp
    tests/integration/recovery_format_tests.cpp
    tests/integration/runtime_concurrency_tests.cpp
    tests/integration/transaction_tests.cpp
    tests/main.cpp
)
target_include_directories(kv_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(kv_test PRIVATE kvstore Threads::Threads)
target_compile_options(kv_test PRIVATE -Wall -Wextra -Wpedantic $<$<CONFIG:Debug>:-g>)
kvstore_apply_sanitizers(kv_test)
kvstore_apply_coverage(kv_test)

add_executable(kv_unit_test
    tests/common/test_support.cpp
    tests/unit/internal_helpers_test.cpp
    tests/unit/unit_main.cpp
    tests/unit/storage_format_test.cpp
    tests/unit/value_cache_test.cpp
)
target_include_directories(kv_unit_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(kv_unit_test PRIVATE kvstore Threads::Threads)
target_compile_options(kv_unit_test PRIVATE -Wall -Wextra -Wpedantic $<$<CONFIG:Debug>:-g>)
kvstore_apply_sanitizers(kv_unit_test)
kvstore_apply_coverage(kv_unit_test)

enable_testing()
add_test(NAME kv_unit_test COMMAND kv_unit_test)
add_test(NAME kv_test COMMAND kv_test)
if (KVSTORE_ENABLE_TSAN)
    # GCC 10's deadlock detector tracks at most 64 concurrently-held locks,
    # while Scan intentionally locks every configured shard for a consistent view.
    # Race detection remains enabled and any report still fails the test.
    set_tests_properties(kv_unit_test kv_test PROPERTIES
        ENVIRONMENT "TSAN_OPTIONS=detect_deadlocks=0:halt_on_error=1")
endif()

install(TARGETS kv_test DESTINATION target/bin)
install(TARGETS kvstore DESTINATION target/lib)
install(FILES include/kvstore.h DESTINATION include)

message(STATUS "Configured KVStore. Build with 'cmake --build <build-dir>' and run tests from the build dir with 'ctest --output-on-failure'.")
