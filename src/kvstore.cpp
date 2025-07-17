#include "kvstore.h"
#include <iostream>
#include <fstream>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <map>
#include <list>
#include <optional>
#include <cstring>
#include <sys/types.h>
#include <unistd.h>


// --- 文件格式定义 ---
#pragma pack(1)
struct DataRecordHeader {
    uint32_t magic;
    int32_t key;
    uint32_t value_size;
};
struct FreeListNode {
    uint32_t magic;
    uint32_t total_size;
    off_t next_offset;
};
struct DBHeader {
    char name[16];
    off_t free_list_head;
};
enum WalRecordType : uint8_t { WAL_PUT, WAL_DELETE };
struct WalRecordHeader {
    WalRecordType type;
    int32_t key;
    uint32_t value_size;
};
#pragma pack()


// --- 缓存模块 (LRU Cache) ---
class LruCache {
public:
    LruCache(size_t capacity) : capacity_(capacity) {}

    std::optional<Value> get(int key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            return std::nullopt;
        }
        list_.splice(list_.begin(), list_, it->second);
        return it->second->second;
    }

    void put(int key, const Value& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second->second = value;
            list_.splice(list_.begin(), list_, it->second);
            return;
        }
        if (map_.size() >= capacity_) {
            int key_to_evict = list_.back().first;
            map_.erase(key_to_evict);
            list_.pop_back();
        }
        list_.emplace_front(key, value);
        map_[key] = list_.begin();
    }

    void erase(int key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            list_.erase(it->second);
            map_.erase(it);
        }
    }

private:
    size_t capacity_;
    std::list<std::pair<int, Value>> list_;
    std::map<int, decltype(list_)::iterator> map_;
    std::mutex mutex_;
};

// --- 核心实现类 ---
class KVStore::Impl {
public:
    enum TaskType { PUT, DELETE, COMPACT };
    struct Task {
        TaskType type;
        int key;
        Value value;
    };

    Impl(const std::string& db_path, int num_consumers)
        : db_file_path_(db_path), wal_file_path_(db_path + ".wal"), stop_(false), cache_(1000) {
        open_db_files();
        recover_from_wal();
        load_metadata();
        for (int i = 0; i < num_consumers; ++i) {
            consumers_.emplace_back(&Impl::consumer_thread_worker, this);
        }
    }

    ~Impl() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            stop_ = true;
        }
        cv_.notify_all();
        for (std::thread& t : consumers_) {
            if (t.joinable()) t.join();
        }
        if (db_file_.is_open()) db_file_.close();
        if (wal_file_.is_open()) wal_file_.close();
    }

    void Put(int key, const Value& value) {
        Task t = {PUT, key, value};
        enqueue_task(t);
    }
    
    void Delete(int key) {
        Task t = {DELETE, key};
        enqueue_task(t);
    }

    void Compact() {
        Task t = {COMPACT};
        enqueue_task(t);
    }

    // --- FIX #1: 修复 Get 与消费者线程的AB-BA死锁 ---
    // 通过规定统一的加锁顺序 (先锁db_mutex_, 后锁cache_mutex_) 来解决死锁问题。
    std::optional<Value> Get(int key) {
        // 1. 乐观地检查一次缓存，这步不锁DB，性能最好
        auto cached_value = cache_.get(key);
        if (cached_value) {
            return cached_value;
        }

        // 2. 缓存未命中。现在必须锁定数据库以从文件读取。
        std::lock_guard<std::mutex> lock(db_mutex_);

        // 3. 关键：在持有数据库锁之后，再次检查缓存，防止“竞赛条件”。
        cached_value = cache_.get(key);
        if (cached_value) {
            return cached_value;
        }

        // 4. 现在可以安全地从文件读取了。
        auto it = metadata_.find(key);
        if (it == metadata_.end()) {
            return std::nullopt;
        }

        db_file_.seekg(it->second.offset);
        DataRecordHeader header;
        db_file_.read(reinterpret_cast<char*>(&header), sizeof(header));

        if (header.magic != 0xDEADBEEF || header.key != key) {
            std::cerr << "ERROR: Corrupted data record for key " << key << std::endl;
            return std::nullopt;
        }

        Value val;
        val.size = header.value_size;
        val.data = std::shared_ptr<char[]>(new char[val.size]);
        db_file_.read(val.data.get(), val.size);

        // 5. 将从磁盘读取的数据放入缓存
        cache_.put(key, val);
        return val;
    }

private:
    struct DataRecordMeta {
        off_t offset;
        uint32_t total_size;
    };
    std::string db_file_path_;
    std::string wal_file_path_;
    std::fstream db_file_;
    std::fstream wal_file_;
    DBHeader db_header_;
    std::map<int, DataRecordMeta> metadata_;
    std::mutex db_mutex_;
    std::queue<Task> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::vector<std::thread> consumers_;
    bool stop_;
    LruCache cache_;

    void open_db_files() {
        db_file_.open(db_file_path_, std::ios::in | std::ios::out | std::ios::binary);
        if (!db_file_.is_open()) {
            db_file_.open(db_file_path_, std::ios::out | std::ios::binary);
            strncpy(db_header_.name, "SimpleKV v1.0", sizeof(db_header_.name));
            db_header_.free_list_head = 0;
            db_file_.write(reinterpret_cast<char*>(&db_header_), sizeof(db_header_));
            db_file_.close();
            db_file_.open(db_file_path_, std::ios::in | std::ios::out | std::ios::binary);
        } else {
            db_file_.read(reinterpret_cast<char*>(&db_header_), sizeof(db_header_));
        }
        wal_file_.open(wal_file_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    }
    
    void recover_from_wal() {
        std::lock_guard<std::mutex> lock(db_mutex_);
        wal_file_.seekg(0);
        std::cout << "Starting WAL recovery..." << std::endl;
        int count = 0;
        while(true) {
            WalRecordHeader header;
            wal_file_.read(reinterpret_cast<char*>(&header), sizeof(header));
            if (wal_file_.eof()) break;

            if (header.type == WAL_PUT) {
                Value val;
                val.size = header.value_size;
                val.data = std::shared_ptr<char[]>(new char[val.size]);
                wal_file_.read(val.data.get(), val.size);
                physical_put(header.key, val);
            } else if (header.type == WAL_DELETE) {
                physical_delete(header.key);
            }
            count++;
        }
        std::cout << "WAL recovery finished. " << count << " records replayed." << std::endl;

        wal_file_.close();
        wal_file_.open(wal_file_path_, std::ios::out | std::ios::trunc);
        wal_file_.close();
        wal_file_.open(wal_file_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    }

    void load_metadata() {
        std::lock_guard<std::mutex> lock(db_mutex_);
        metadata_.clear();
        db_file_.seekg(sizeof(DBHeader));
        std::cout << "Loading metadata from DB file..." << std::endl;
        while(true) {
            off_t current_offset = db_file_.tellg();
            DataRecordHeader header;
            db_file_.read(reinterpret_cast<char*>(&header), sizeof(header));
            if (db_file_.eof()) break;

            if (header.magic == 0xDEADBEEF) {
                metadata_[header.key] = {current_offset, static_cast<uint32_t>(sizeof(DataRecordHeader) + header.value_size)};
                db_file_.seekg(header.value_size, std::ios::cur);
            } else if (header.magic == 0xCAFEFEED) {
                 FreeListNode node;
                 db_file_.seekg(- (long)sizeof(header.magic), std::ios::cur);
                 db_file_.read(reinterpret_cast<char*>(&node), sizeof(node));
                 db_file_.seekg(node.total_size - sizeof(FreeListNode), std::ios::cur);
            } else {
                 break;
            }
        }
        std::cout << "Metadata loaded. " << metadata_.size() << " keys found." << std::endl;
    }

    void enqueue_task(const Task& task) {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            task_queue_.push(task);
        }
        cv_.notify_one();
    }
    
    void consumer_thread_worker() {
        while (true) {
            Task task;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                cv_.wait(lock, [this] { return !task_queue_.empty() || stop_; });
                if (stop_ && task_queue_.empty()) return;
                task = task_queue_.front();
                task_queue_.pop();
            }

            std::lock_guard<std::mutex> lock(db_mutex_);
            if (task.type == PUT) {
                wal_log_put(task.key, task.value);
                physical_put(task.key, task.value);
            } else if (task.type == DELETE) {
                wal_log_delete(task.key);
                physical_delete(task.key);
            } else if (task.type == COMPACT) {
                handle_compaction();
            }
        }
    }
    
    void wal_log_put(int key, const Value& value) {
        WalRecordHeader header = {WAL_PUT, key, (uint32_t)value.size};
        wal_file_.write(reinterpret_cast<char*>(&header), sizeof(header));
        wal_file_.write(value.data.get(), value.size);
        wal_file_.flush();
    }

    void wal_log_delete(int key) {
        WalRecordHeader header = {WAL_DELETE, key, 0};
        wal_file_.write(reinterpret_cast<char*>(&header), sizeof(header));
        wal_file_.flush();
    }

    void physical_put(int key, const Value& value) {
        if (metadata_.count(key)) {
            physical_delete(key);
        }
        uint32_t required_size = sizeof(DataRecordHeader) + value.size;
        off_t write_offset = find_free_space(required_size);
        db_file_.seekp(write_offset);
        DataRecordHeader header = {0xDEADBEEF, key, (uint32_t)value.size};
        db_file_.write(reinterpret_cast<char*>(&header), sizeof(header));
        db_file_.write(value.data.get(), value.size);
        metadata_[key] = {write_offset, required_size};
        cache_.put(key, value);
    }
    
    void physical_delete(int key) {
        auto it = metadata_.find(key);
        if (it == metadata_.end()) return;
        DataRecordMeta meta = it->second;
        FreeListNode free_node;
        free_node.magic = 0xCAFEFEED;
        free_node.total_size = meta.total_size;
        free_node.next_offset = db_header_.free_list_head;
        db_file_.seekp(meta.offset);
        db_file_.write(reinterpret_cast<char*>(&free_node), sizeof(free_node));
        db_header_.free_list_head = meta.offset;
        db_file_.seekp(0);
        db_file_.write(reinterpret_cast<char*>(&db_header_), sizeof(db_header_));
        metadata_.erase(it);
        cache_.erase(key);
    }

    off_t find_free_space(uint32_t required_size) {
        off_t current_offset = db_header_.free_list_head;
        off_t prev_offset = 0;
        while(current_offset != 0) {
            db_file_.seekg(current_offset);
            FreeListNode node;
            db_file_.read(reinterpret_cast<char*>(&node), sizeof(node));
            if (node.total_size >= required_size) {
                if (prev_offset == 0) {
                    db_header_.free_list_head = node.next_offset;
                    db_file_.seekp(0);
                    db_file_.write(reinterpret_cast<char*>(&db_header_), sizeof(db_header_));
                } else {
                    FreeListNode prev_node;
                    db_file_.seekg(prev_offset);
                    db_file_.read(reinterpret_cast<char*>(&prev_node), sizeof(prev_node));
                    prev_node.next_offset = node.next_offset;
                    db_file_.seekp(prev_offset);
                    db_file_.write(reinterpret_cast<char*>(&prev_node), sizeof(prev_node));
                }
                return current_offset;
            }
            prev_offset = current_offset;
            current_offset = node.next_offset;
        }
        db_file_.seekp(0, std::ios::end);
        return db_file_.tellp();
    }
    
    // --- FIX #2: 修复 Compaction 内部调用 Get 导致的递归锁死锁 ---
    // 新的实现不再调用 Get()，而是直接从旧的数据文件读取裸数据并写入新文件，
    // 从而避免了对 db_mutex_ 的重复加锁。
    void handle_compaction() {
        std::cout << "Starting compaction..." << std::endl;
        std::string compact_db_path = db_file_path_ + ".compact";
        
        std::fstream compact_db_file(compact_db_path, std::ios::out | std::ios::binary);
        if (!compact_db_file.is_open()) {
            std::cerr << "ERROR: Failed to create compaction file." << std::endl;
            return;
        }

        DBHeader new_header;
        strncpy(new_header.name, "SimpleKV v1.0", sizeof(new_header.name));
        new_header.free_list_head = 0;
        compact_db_file.write(reinterpret_cast<char*>(&new_header), sizeof(new_header));
        
        std::map<int, DataRecordMeta> new_metadata;
        Value val_buffer; // 可复用的 value buffer

        for (const auto& pair : metadata_) {
            int key = pair.first;
            const DataRecordMeta& meta = pair.second;
            
            // 直接从旧文件读取数据，不调用 Get()
            db_file_.seekg(meta.offset);
            DataRecordHeader data_header;
            db_file_.read(reinterpret_cast<char*>(&data_header), sizeof(data_header));
            
            // 验证一下我们读到的数据是否正确
            if (data_header.magic != 0xDEADBEEF || data_header.key != key) {
                continue; // 跳过损坏或不一致的数据
            }
            
            val_buffer.size = data_header.value_size;
            val_buffer.data.reset(new char[val_buffer.size]);
            db_file_.read(val_buffer.data.get(), val_buffer.size);

            // 将干净的数据写入新文件
            off_t write_offset = compact_db_file.tellp();
            compact_db_file.write(reinterpret_cast<char*>(&data_header), sizeof(data_header));
            compact_db_file.write(val_buffer.data.get(), val_buffer.size);

            new_metadata[key] = {write_offset, meta.total_size};
        }
        
        compact_db_file.close();

        // 原子地替换旧文件
        db_file_.close();
        wal_file_.close();
        
        remove(wal_file_path_.c_str());
        rename(compact_db_path.c_str(), db_file_path_.c_str());

        // 重新打开文件并加载新元数据
        metadata_ = new_metadata;
        open_db_files();

        std::cout << "Compaction finished." << std::endl;
    }

};

// --- KVStore 公共方法实现 (不变) ---
KVStore::KVStore(const std::string& db_path, int num_consumers)
    : pimpl_(std::make_unique<Impl>(db_path, num_consumers)) {}

KVStore::~KVStore() = default;

void KVStore::Put(int key, const Value& value) { pimpl_->Put(key, value); }
std::optional<Value> KVStore::Get(int key) { return pimpl_->Get(key); }
void KVStore::Delete(int key) { pimpl_->Delete(key); }
void KVStore::Compact() { pimpl_->Compact(); }