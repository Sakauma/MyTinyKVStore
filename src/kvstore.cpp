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
#include <atomic>
#include <set>

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
        : db_file_path_(db_path), wal_file_path_(db_path + ".wal"), stop_(false), fatal_error_(false), cache_(1000) {
        open_db_files();
        if (fatal_error_) return;
        load_metadata();  
        if (fatal_error_) return;
        recover_from_wal();
        if (fatal_error_) return;

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
        
        // 在关闭前确保所有WAL日志都已落盘
        if (wal_file_.is_open() && !fatal_error_) {
            wal_file_.flush();
        }

        if (db_file_.is_open()) db_file_.close();
        if (wal_file_.is_open()) wal_file_.close();
    }

    void Put(int key, const Value& value) {
        if (fatal_error_) return;
        Task t = {PUT, key, value};
        enqueue_task(t);
    }
    
    void Delete(int key) {
        if (fatal_error_) return;
        Task t = {DELETE, key};
        enqueue_task(t);
    }

    void Compact() {
        if (fatal_error_) return;
        Task t = {COMPACT};
        enqueue_task(t);
    }
    
    std::optional<Value> Get(int key) {
        if (fatal_error_) return std::nullopt;

        // FIX: 优先检查是否已被逻辑删除
        if (deleted_keys_.count(key)) {
            return std::nullopt;
        }

        auto cached_value = cache_.get(key);
        if (cached_value) {
            return cached_value;
        }

        std::lock_guard<std::mutex> lock(db_mutex_);
        if (fatal_error_) return std::nullopt;

        cached_value = cache_.get(key);
        if (cached_value) {
            return cached_value;
        }

        auto it = metadata_.find(key);
        if (it == metadata_.end()) {
            return std::nullopt;
        }
        
        db_file_.seekg(it->second.offset);
        if (!db_file_) { handle_io_error("Get: seekg failed"); return std::nullopt; }

        DataRecordHeader header;
        db_file_.read(reinterpret_cast<char*>(&header), sizeof(header));
        if (!db_file_) { handle_io_error("Get: read header failed"); return std::nullopt; }

        if (header.magic != 0xDEADBEEF || header.key != key) {
            std::cerr << "ERROR: Corrupted data record for key " << key << std::endl;
            return std::nullopt;
        }

        Value val;
        val.size = header.value_size;
        val.data = std::shared_ptr<char[]>(new char[val.size]);
        db_file_.read(val.data.get(), val.size);
        if (!db_file_) { handle_io_error("Get: read value failed"); return std::nullopt; }

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
    std::set<int> deleted_keys_;
    std::mutex db_mutex_;
    std::queue<Task> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::vector<std::thread> consumers_;
    bool stop_;
    std::atomic<bool> fatal_error_;
    LruCache cache_;

    void handle_io_error(const std::string& context) {
        if (fatal_error_) return;
        std::cerr << "FATAL I/O ERROR: " << context << ". Halting operations." << std::endl;
        fatal_error_ = true; 
    }

    void open_db_files() {
        db_file_.open(db_file_path_, std::ios::in | std::ios::out | std::ios::binary);
        if (!db_file_.is_open()) {
            db_file_.open(db_file_path_, std::ios::out | std::ios::binary);
            if (!db_file_.is_open()) { handle_io_error("Failed to create DB file"); return; }
            strncpy(db_header_.name, "SimpleKV v1.0", sizeof(db_header_.name));
            db_header_.free_list_head = 0;
            db_file_.write(reinterpret_cast<char*>(&db_header_), sizeof(db_header_));
            if (!db_file_) { handle_io_error("Failed to write initial DB header"); return; }
            db_file_.close();
            db_file_.open(db_file_path_, std::ios::in | std::ios::out | std::ios::binary);
            if (!db_file_.is_open()) { handle_io_error("Failed to reopen DB file"); return; }
        } else {
            db_file_.read(reinterpret_cast<char*>(&db_header_), sizeof(db_header_));
            if (!db_file_) { handle_io_error("Failed to read DB header"); return; }
        }
        wal_file_.open(wal_file_path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
        if (!wal_file_.is_open()) { handle_io_error("Failed to open WAL file"); return; }
    }

    void recover_from_wal() {
        std::lock_guard<std::mutex> lock(db_mutex_);
        wal_file_.seekg(0);
        if (!wal_file_) { handle_io_error("WAL recovery: seekg failed"); return; }
        
        std::cout << "Starting WAL recovery..." << std::endl;
        int count = 0;
        while(true) {
            WalRecordHeader header;
            wal_file_.read(reinterpret_cast<char*>(&header), sizeof(header));
            if (wal_file_.eof()) break;
            if (wal_file_.fail()) { handle_io_error("WAL recovery: read header failed"); return; }

            if (header.type == WAL_PUT) {
                // 当日志中有一个PUT操作，我们只需确保它不在“已删除”集合里即可。
                // 我们不调用physical_put，因为load_metadata已经从.dat文件加载了它。
                deleted_keys_.erase(header.key);

                // 跳过value数据，因为我们不需要它
                wal_file_.seekg(header.value_size, std::ios::cur);
                if (!wal_file_) { handle_io_error("WAL recovery: seek past value failed"); return; }

            } else if (header.type == WAL_DELETE) {
                // 当日志中有一个DELETE操作，我们更新内存状态
                deleted_keys_.insert(header.key);
                //metadata_.erase(header.key);
                cache_.erase(header.key);
            }
            count++;
        }
        std::cout << "WAL recovery finished. " << count << " records replayed." << std::endl;
        
        wal_file_.clear();
    }

    void load_metadata() {
        std::lock_guard<std::mutex> lock(db_mutex_);
        metadata_.clear();
        db_file_.seekg(0, std::ios::end);
        if (!db_file_) { handle_io_error("Load metadata: seek to end failed"); return; }
        off_t file_size = db_file_.tellg();

        db_file_.seekg(sizeof(DBHeader));
        if (!db_file_) { handle_io_error("Load metadata: seek to start failed"); return; }
        
        std::cout << "Loading metadata from DB file..." << std::endl;
        
        while (db_file_.tellg() < file_size) {
            off_t current_offset = db_file_.tellg();
            DataRecordHeader header;
            db_file_.read(reinterpret_cast<char*>(&header), sizeof(header));
            if (db_file_.eof()) break;
            if (db_file_.fail()) { handle_io_error("Load metadata: read header failed"); return; }

            if (header.magic == 0xDEADBEEF) {
                metadata_[header.key] = {current_offset, static_cast<uint32_t>(sizeof(DataRecordHeader) + header.value_size)};
                db_file_.seekg(current_offset + sizeof(DataRecordHeader) + header.value_size);
                if (!db_file_) { handle_io_error("Load metadata: seek past data record failed"); return; }
            } else {
                std::cerr << "WARN: Unknown magic number encountered at offset " << current_offset << ". Stopping metadata load." << std::endl;
                break; 
            }
        }
        db_file_.clear();
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
                if (task_queue_.empty() && !stop_) {
                    wal_file_.flush();
                    if(!wal_file_) handle_io_error("Consumer flush failed");
                }
                cv_.wait(lock, [this] { return !task_queue_.empty() || stop_; });
                if (stop_ && task_queue_.empty()) return;
                task = task_queue_.front();
                task_queue_.pop();
            }
            if (fatal_error_) continue;

            std::lock_guard<std::mutex> lock(db_mutex_);
            if (fatal_error_) continue;

            if (task.type == PUT) {
                wal_log_put(task.key, task.value);
                if(fatal_error_) continue;
                physical_put(task.key, task.value);
            } else if (task.type == DELETE) {
                wal_log_delete(task.key);
                if(fatal_error_) continue;
                physical_delete(task.key); // 现在是逻辑删除
            } else if (task.type == COMPACT) {
                handle_compaction();
            }
        }
    }

    void wal_log_put(int key, const Value& value) {
        WalRecordHeader header = {WAL_PUT, key, (uint32_t)value.size};
        wal_file_.write(reinterpret_cast<char*>(&header), sizeof(header));
        if (!wal_file_) { handle_io_error("WAL log put: write header failed"); return; }
        wal_file_.write(value.data.get(), value.size);
        if (!wal_file_) { handle_io_error("WAL log put: write value failed"); return; }
    }
    
    void wal_log_delete(int key) {
        WalRecordHeader header = {WAL_DELETE, key, 0};
        wal_file_.write(reinterpret_cast<char*>(&header), sizeof(header));
        if (!wal_file_) { handle_io_error("WAL log delete: write header failed"); return; }
    }

    void physical_put(int key, const Value& value) {
        if (metadata_.count(key)) {
        }
        
        db_file_.seekp(0, std::ios::end);
        if (!db_file_) { handle_io_error("Physical put: seekp to end failed"); return; }
        off_t write_offset = db_file_.tellp();

        DataRecordHeader header = {0xDEADBEEF, key, (uint32_t)value.size};
        db_file_.write(reinterpret_cast<char*>(&header), sizeof(header));
        if (!db_file_) { handle_io_error("Physical put: write header failed"); return; }

        db_file_.write(value.data.get(), value.size);
        if (!db_file_) { handle_io_error("Physical put: write value failed"); return; }

        metadata_[key] = {write_offset, static_cast<uint32_t>(sizeof(DataRecordHeader) + value.size)};
        cache_.put(key, value);
        deleted_keys_.erase(key);
    }

    void physical_delete(int key) {
        if (metadata_.count(key)) {
            deleted_keys_.insert(key);
            metadata_.erase(key);
            cache_.erase(key);
        }
    }

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
        new_header.free_list_head = 0; // 新文件没有自由链表
        compact_db_file.write(reinterpret_cast<char*>(&new_header), sizeof(new_header));
        if (!compact_db_file) {
            std::cerr << "ERROR: Compaction: failed to write new header." << std::endl;
            compact_db_file.close(); remove(compact_db_path.c_str()); return;
        }
        
        std::map<int, DataRecordMeta> new_metadata;
        std::vector<char> val_buffer;

        // 遍历所有未被删除的key
        for (const auto& pair : metadata_) {
            int key = pair.first;
            // 跳过已删除的key (虽然理论上它们已经从metadata_中移除，但双重保险)
            if (deleted_keys_.count(key)) continue; 
            
            const DataRecordMeta& meta = pair.second;
            
            db_file_.seekg(meta.offset);
            if (!db_file_) { handle_io_error("Compaction: seekg to old record failed"); return; }
            DataRecordHeader data_header;
            db_file_.read(reinterpret_cast<char*>(&data_header), sizeof(data_header));
            if (db_file_.fail()) { handle_io_error("Compaction: read old header failed"); return; }
            
            if (data_header.magic != 0xDEADBEEF || data_header.key != key) {
                std::cerr << "WARN: Compaction: skipping corrupted record for key " << key << std::endl;
                continue;
            }
            
            if (val_buffer.size() < data_header.value_size) {
                val_buffer.resize(data_header.value_size);
            }
            db_file_.read(val_buffer.data(), data_header.value_size);
            if (db_file_.fail()) { handle_io_error("Compaction: read old value failed"); return; }

            off_t write_offset = compact_db_file.tellp();
            compact_db_file.write(reinterpret_cast<char*>(&data_header), sizeof(data_header));
            if (!compact_db_file) { handle_io_error("Compaction: write new header failed"); return; }
            compact_db_file.write(val_buffer.data(), data_header.value_size);
            if (!compact_db_file) { handle_io_error("Compaction: write new value failed"); return; }

            new_metadata[key] = {write_offset, meta.total_size};
        }
        
        compact_db_file.close();

        db_file_.close();
        wal_file_.close();
        
        if(remove(db_file_path_.c_str()) != 0 || remove(wal_file_path_.c_str()) != 0) {
            std::cerr << "ERROR: Compaction: failed to remove old files." << std::endl;
        }
        if(rename(compact_db_path.c_str(), db_file_path_.c_str()) != 0) {
            std::cerr << "ERROR: Compaction: failed to rename compact file." << std::endl;
            fatal_error_ = true;
            return;
        }
        
        metadata_ = new_metadata;
        deleted_keys_.clear(); // 清空删除标记
        open_db_files(); // 重新打开文件句柄，特别是截断并重建WAL

        std::cout << "Compaction finished." << std::endl;
    }
};

// --- KVStore 公共方法实现 ---
KVStore::KVStore(const std::string& db_path, int num_consumers)
    : pimpl_(std::make_unique<Impl>(db_path, num_consumers)) {}

KVStore::~KVStore() = default;

void KVStore::Put(int key, const Value& value) { pimpl_->Put(key, value); }
std::optional<Value> KVStore::Get(int key) { return pimpl_->Get(key); }
void KVStore::Delete(int key) { pimpl_->Delete(key); }
void KVStore::Compact() { pimpl_->Compact(); }