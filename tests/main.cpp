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
#include <algorithm> 
#include <numeric>  
#include <variant>   
#include <map>
#include <set>

// 序列化标签
enum class DataType : uint8_t {
    STRING = 0,
    INT32  = 1,
    DOUBLE = 2,
};

// 根据类型标签打印值
void print_value(const Value& val) {
    if (val.size < 1) {
        std::cout << "[INVALID DATA]";
        return;
    }
    // 读取第一个字节作为类型标签
    DataType type = static_cast<DataType>(val.data.get()[0]);
    const char* raw_data = val.data.get() + 1;
    size_t data_size = val.size - 1;

    switch(type) {
        case DataType::STRING: {
            std::cout << "(string): \"" << std::string(raw_data, data_size) << "\"";
            break;
        }
        case DataType::INT32: {
            if (data_size != sizeof(int32_t)) { std::cout << "[INVALID INT32]"; break; }
            int32_t int_val;
            memcpy(&int_val, raw_data, sizeof(int32_t));
            std::cout << "(int32): " << int_val;
            break;
        }
        case DataType::DOUBLE: {
            if (data_size != sizeof(double)) { std::cout << "[INVALID DOUBLE]"; break; }
            double double_val;
            memcpy(&double_val, raw_data, sizeof(double));
            std::cout << "(double): " << double_val;
            break;
        }
        default: {
            std::cout << "[UNKNOWN TYPE]";
            break;
        }
    }
}

// --- 全局原子计数器，用于跨线程统计 ---
std::atomic<uint64_t> puts_done(0);
std::atomic<uint64_t> gets_done(0);
std::atomic<uint64_t> deletes_done(0);
std::atomic<uint64_t> verification_failures(0);
std::atomic<bool> stop_test(false);

// --- 生成测试数据 ---
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
                if (op_distrib(gen) < 1) { // 1%的概率进行验证
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
        if (elapsed_since_last_report == 0) elapsed_since_last_report = 1;
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

// +++ 测试函数 +++
void demo_test() {
    std::cout << "--- Running Demonstration Test (with Mixed Data Types) ---" << std::endl;
    const char* db_name = "demo_db.dat";

    remove(db_name);
    remove((std::string(db_name) + ".wal").c_str());

    std::map<int, DataType> inserted_items;
    const int num_to_insert = 20;
    const int num_to_delete = 5;

    std::mt19937 gen(1337); 
    std::uniform_int_distribution<> key_distrib(1, 10000);
    std::uniform_int_distribution<> type_distrib(0, 2); // 0: string, 1: int32, 2: double

    // --- 阶段1: 插入不同类型的键值对 ---
    std::cout << "\n[PHASE 1] Inserting " << num_to_insert << " key-value pairs of random types..." << std::endl;
    {
        KVStore store(db_name);
        std::set<int> unique_keys;
        while(unique_keys.size() < num_to_insert) {
            unique_keys.insert(key_distrib(gen));
        }

        for (int key : unique_keys) {
            DataType type = static_cast<DataType>(type_distrib(gen));
            inserted_items[key] = type;
            
            Value val;
            
            // 根据随机类型创建并序列化 Value
            switch(type) {
                case DataType::STRING: {
                    std::string s = "key_" + std::to_string(key);
                    val.size = 1 + s.length();
                    val.data.reset(new char[val.size]);
                    val.data.get()[0] = static_cast<uint8_t>(DataType::STRING);
                    memcpy(val.data.get() + 1, s.c_str(), s.length());
                    break;
                }
                case DataType::INT32: {
                    int32_t i = key * 10;
                    val.size = 1 + sizeof(int32_t);
                    val.data.reset(new char[val.size]);
                    val.data.get()[0] = static_cast<uint8_t>(DataType::INT32);
                    memcpy(val.data.get() + 1, &i, sizeof(int32_t));
                    break;
                }
                case DataType::DOUBLE: {
                    double d = key / 3.14;
                    val.size = 1 + sizeof(double);
                    val.data.reset(new char[val.size]);
                    val.data.get()[0] = static_cast<uint8_t>(DataType::DOUBLE);
                    memcpy(val.data.get() + 1, &d, sizeof(double));
                    break;
                }
            }
            store.Put(key, val);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        std::cout << "Insertion complete." << std::endl;
    } // DB关闭

    // --- 阶段2: 验证持久化和数据类型 ---
    std::cout << "\n[PHASE 2] Reopening DB to verify data..." << std::endl;
    {
        KVStore store(db_name);
        std::cout << "Reading all inserted keys:" << std::endl;
        for(auto const& [key, type] : inserted_items) {
            auto result = store.Get(key);
            assert(result.has_value());
            std::cout << "  Key: " << std::setw(5) << key << " -> Value: ";
            print_value(*result);
            std::cout << std::endl;
        }
        std::cout << "Initial verification PASSED." << std::endl;
    } // DB关闭

    // --- 阶段3: 删除并最终验证 ---
    std::cout << "\n[PHASE 3] Deleting " << num_to_delete << " keys and final verification..." << std::endl;
    {
        KVStore store(db_name);
        std::vector<int> keys_to_delete;
        auto it = inserted_items.begin();
        for (int i = 0; i < num_to_delete; ++i) {
            store.Delete(it->first);   // 直接删除
            ++it;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        std::cout << "Deleted keys: ";
        for(int k : keys_to_delete) { std::cout << k << " "; }
        std::cout << std::endl;
    } // DB关闭

    std::cout << "\nVerifying final state after deletion:" << std::endl;
    {
        KVStore store(db_name);
        int found_count = 0;
        for(auto const& [key, type] : inserted_items) {
            auto result = store.Get(key);
            if (!result.has_value()) {
            std::cout << "  Deleted - Key: " << key << std::endl;
            } else {
                std::cout << "  Exists  - Key: " << std::setw(5) << key << " -> Value: ";
                print_value(*result);
                std::cout << std::endl;
                found_count++;
            }
        }
        assert(found_count <= num_to_insert);
        assert(found_count >= num_to_insert - num_to_delete);
        std::cout << "Final verification PASSED." << std::endl;
    }
    
    std::cout << "\n---Demonstration Test Finished Successfully! ---" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc > 1) {
        std::string arg = argv[1];
        if (arg == "stress") {
            stress_test();
        } else if (arg == "demo") {
            demo_test();
        } else {
            std::cout << "Unknown command: " << arg << std::endl;
            std::cout << "Usage: ./kv_test [stress|demo]" << std::endl;
        }
    } else {
        std::cout << "Running basic tests. Use './kv_test demo' for a demonstration or './kv_test stress' for the long-running test." << std::endl;
        simple_test();
    }
    return 0;
}