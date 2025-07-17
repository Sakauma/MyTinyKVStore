#include "kvstore.h"
#include <iostream>
#include <thread>
#include <vector>
#include <cstring>
#include <cassert>
#include <chrono>
#include <random>
#include <atomic>
#include <iomanip>

// --- 全局原子计数器，用于跨线程统计 ---
std::atomic<uint64_t> puts_done(0);
std::atomic<uint64_t> gets_done(0);
std::atomic<uint64_t> deletes_done(0);
std::atomic<uint64_t> verification_failures(0);
std::atomic<bool> stop_test(false);

// --- 简单的辅助函数，用于生成测试数据 ---
std::string generate_value_from_key(int key) {
    return "value_for_key_" + std::to_string(key) + "_with_some_padding_to_make_it_longer";
}

// --- 写入线程的工作函数 ---
void writer_worker(KVStore& store, int key_start, int key_end) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(key_start, key_end - 1);

    while (!stop_test) {
        int key = distrib(gen);
        std::string s_val = generate_value_from_key(key);
        
        Value val;
        val.size = s_val.length();
        val.data.reset(new char[val.size]);
        memcpy(val.data.get(), s_val.c_str(), val.size);
        
        store.Put(key, val);
        puts_done++;
        std::this_thread::sleep_for(std::chrono::microseconds(100)); // 控制写入速度
    }
}

// --- 读/删线程的工作函数 ---
void reader_deleter_worker(KVStore& store, int key_range_max) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> key_distrib(0, key_range_max - 1);
    std::uniform_int_distribution<> op_distrib(0, 99);

    while (!stop_test) {
        int key = key_distrib(gen);
        int operation = op_distrib(gen);

        if (operation < 90) {
            auto result = store.Get(key);
            gets_done++;
            if (result.has_value()) {
                if (op_distrib(gen) < 1) {
                    std::string expected_val_str = generate_value_from_key(key);
                    Value expected_val;
                    expected_val.size = expected_val_str.length();
                    expected_val.data.reset(new char[expected_val.size]);
                    memcpy(expected_val.data.get(), expected_val_str.c_str(), expected_val.size);
                    if (!(*result == expected_val)) {
                        verification_failures++;
                    }
                }
            }
        } else { 
            store.Delete(key);
            deletes_done++;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(50)); // 控制读/删速度
    }
}

// --- 压力测试函数 ---
void stress_test() {
    std::cout << "--- Starting 12-Hour Stress Test ---" << std::endl;
    const auto test_start_time = std::chrono::steady_clock::now();
    const auto test_end_time = test_start_time + std::chrono::hours(12);
    
    // 清理环境
    const char* db_name = "stress_test.dat";
    remove(db_name);
    remove((std::string(db_name) + ".wal").c_str());

    // 初始化KVStore
    const int num_writers = 4;
    const int num_readers_deleters = 8;
    const int key_range = 15000000; // 键空间范围，确保能插入超过1000万个唯一对象
    KVStore store(db_name, 8); // 使用8个消费者线程

    // 启动工作线程
    std::vector<std::thread> threads;
    for (int i = 0; i < num_writers; ++i) {
        threads.emplace_back(writer_worker, std::ref(store), i * (key_range/num_writers), (i+1) * (key_range/num_writers));
    }
    for (int i = 0; i < num_readers_deleters; ++i) {
        threads.emplace_back(reader_deleter_worker, std::ref(store), key_range);
    }
    
    // 监控和报告循环
    auto last_report_time = test_start_time;
    auto last_compact_time = test_start_time;
    uint64_t last_puts = 0, last_gets = 0, last_deletes = 0;

    while (std::chrono::steady_clock::now() < test_end_time) {
        std::this_thread::sleep_for(std::chrono::seconds(60)); // 每分钟报告一次
        
        auto now = std::chrono::steady_clock::now();
        auto elapsed_since_last_report = std::chrono::duration_cast<std::chrono::seconds>(now - last_report_time).count();
        auto total_elapsed = std::chrono::duration_cast<std::chrono::minutes>(now - test_start_time);
        
        uint64_t current_puts = puts_done.load();
        uint64_t current_gets = gets_done.load();
        uint64_t current_deletes = deletes_done.load();

        double put_rate = (double)(current_puts - last_puts) / elapsed_since_last_report;
        double get_rate = (double)(current_gets - last_gets) / elapsed_since_last_report;
        double delete_rate = (double)(current_deletes - last_deletes) / elapsed_since_last_report;

        std::cout << "---[ " << std::setw(4) << total_elapsed.count() << " min ]---"
                  << " Puts: " << std::setw(9) << current_puts << " (" << std::fixed << std::setprecision(2) << std::setw(7) << put_rate << "/s) |"
                  << " Gets: " << std::setw(9) << current_gets << " (" << std::fixed << std::setprecision(2) << std::setw(7) << get_rate << "/s) |"
                  << " Dels: " << std::setw(9) << current_deletes << " (" << std::fixed << std::setprecision(2) << std::setw(7) << delete_rate << "/s) |"
                  << " Fails: " << verification_failures.load() 
                  << std::endl;

        last_puts = current_puts;
        last_gets = current_gets;
        last_deletes = current_deletes;
        last_report_time = now;
        
        // 每小时触发一次Compaction
        if (std::chrono::duration_cast<std::chrono::hours>(now - last_compact_time).count() >= 1) {
            std::cout << ">>> Triggering hourly compaction..." << std::endl;
            store.Compact();
            std::cout << ">>> Compaction finished." << std::endl;
            last_compact_time = now;
        }

        // 检查是否已插入足够对象
        if (current_puts >= 10000000) {
            std::cout << ">>> Target of 10 million objects inserted has been reached." << std::endl;
        }
    }
    
    // 停止所有线程
    stop_test = true;
    for (auto& t : threads) {
        t.join();
    }
    
    std::cout << "--- 12-Hour Stress Test Finished ---" << std::endl;
    std::cout << "Total Puts: " << puts_done.load() << std::endl;
    std::cout << "Total Gets: " << gets_done.load() << std::endl;
    std::cout << "Total Deletes: " << deletes_done.load() << std::endl;
    std::cout << "Verification Failures: " << verification_failures.load() << std::endl;
}

// --- 一般测试函数 ---
void simple_test() {
    std::cout << "--- Running Simple Test ---" << std::endl;
    // 清理旧文件
    remove("test_db.dat");
    remove("test_db.dat.wal");

    Value val1;
    std::string s1 = "hello";
    val1.size = s1.length();
    val1.data.reset(new char[val1.size]);
    memcpy(val1.data.get(), s1.c_str(), val1.size);

    Value val2;
    std::string s2 = "world";
    val2.size = s2.length();
    val2.data.reset(new char[val2.size]);
    memcpy(val2.data.get(), s2.c_str(), val2.size);

    // 1. 创建DB并写入数据
    {
        KVStore store("test_db.dat");
        store.Put(100, val1);
        store.Put(200, val2);
        // 等待任务处理
        std::this_thread::sleep_for(std::chrono::seconds(1));
    } // store析构，自动关闭

    // 2. 重新打开DB，验证数据是否存在
    {
        KVStore store("test_db.dat");
        auto res1 = store.Get(100);
        auto res2 = store.Get(200);
        assert(res1.has_value() && *res1 == val1);
        assert(res2.has_value() && *res2 == val2);
        std::cout << "Persistence Test PASSED." << std::endl;
        
        // 测试删除和更新
        store.Delete(100);
        store.Put(200, val1); // Update 200 with val1
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    // 3. 再次打开，验证删除和更新
    {
        KVStore store("test_db.dat");
        auto res1 = store.Get(100);
        auto res2 = store.Get(200);
        assert(!res1.has_value());
        assert(res2.has_value() && *res2 == val1);
        std::cout << "Delete and Update Test PASSED." << std::endl;
    }
    std::cout << "---------------------------\n" << std::endl;
}


int main(int argc, char* argv[]) {
    if (argc > 1 && std::string(argv[1]) == "stress") {
        stress_test();
    } else {
        std::cout << "Running basic tests. Use './kv_test stress' for the long-running test." << std::endl;
        simple_test();
    }
    return 0;
}