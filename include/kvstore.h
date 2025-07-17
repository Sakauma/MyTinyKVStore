#ifndef KVSTORE_H
#define KVSTORE_H

#include <cstring>
#include <string>
#include <vector>
#include <memory>
#include <optional>

struct Value {
    std::shared_ptr<char[]> data;
    size_t size;

    bool operator==(const Value& other) const {
        if (size != other.size) return false;
        return memcmp(data.get(), other.data.get(), size) == 0;
    }
};

class KVStore {
public:
    // db_path: 数据库文件路径
    // num_consumers: 消费者线程数
    KVStore(const std::string& db_path, int num_consumers = 4);

    ~KVStore();

    // 异步的PUT操作
    void Put(int key, const Value& value);

    // 同步的GET操作
    std::optional<Value> Get(int key);

    // 异步的DELETE操作
    void Delete(int key);
    
    // 提供一个手动触发compact的接口
    void Compact();

private:
    class Impl;
    std::unique_ptr<Impl> pimpl_;
};

#endif // KVSTORE_H