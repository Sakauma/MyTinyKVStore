#include "tests/integration/test_registry.h"

#include "kvstore.h"

#include <vector>

namespace kvstore::tests::integration {
namespace {

using test_support::as_string;
using test_support::require;
using test_support::TestDir;
using test_support::text;

void test_basic_persistence() {
    TestDir dir("basic");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(1, text("alpha"));
        store.Put(2, text("beta"));
    }

    KVStore reopened(db_path);
    const auto value1 = reopened.Get(1);
    const auto value2 = reopened.Get(2);
    require(value1.has_value(), "key 1 should exist after reopen");
    require(value2.has_value(), "key 2 should exist after reopen");
    require(as_string(*value1) == "alpha", "key 1 should keep its value");
    require(as_string(*value2) == "beta", "key 2 should keep its value");
}

void test_string_keys_do_not_collide_with_int_keys() {
    TestDir dir("string_keys");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.Put(42, text("int-key"));
        store.Put(std::string("42"), text("string-key"));
        store.Put(std::string("alpha"), text("word"));
    }

    KVStore reopened(db_path);
    const auto int_value = reopened.Get(42);
    const auto string_value = reopened.Get(std::string("42"));
    const auto alpha_value = reopened.Get(std::string("alpha"));
    require(int_value.has_value(), "int key should persist");
    require(string_value.has_value(), "string key should persist");
    require(alpha_value.has_value(), "string word key should persist");
    require(as_string(*int_value) == "int-key", "int key should keep its own namespace");
    require(as_string(*string_value) == "string-key", "string key should not collide with int namespace");
    require(as_string(*alpha_value) == "word", "string API should round-trip normal string keys");
}

void test_string_scan_returns_sorted_range() {
    TestDir dir("string_scan");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    store.Put(std::string("apple"), text("a"));
    store.Put(std::string("banana"), text("b"));
    store.Put(std::string("carrot"), text("c"));
    store.Put(std::string("date"), text("d"));
    store.Put(7, text("int-should-not-appear"));

    const auto results = store.Scan("banana", "date");
    require(results.size() == 3, "scan should return all string keys in the requested inclusive range");
    require(results[0].first == "banana" && as_string(results[0].second) == "b", "scan should start at banana");
    require(results[1].first == "carrot" && as_string(results[1].second) == "c", "scan should keep lexical order");
    require(results[2].first == "date" && as_string(results[2].second) == "d", "scan should include the upper bound");
}

void test_binary_keys_do_not_collide_with_other_namespaces() {
    TestDir dir("binary_keys");
    const std::string db_path = dir.file("store.dat");
    const std::vector<uint8_t> binary_42 = {'4', '2'};
    const std::vector<uint8_t> binary_raw = {0x00, 0xFF, 0x42};

    {
        KVStore store(db_path);
        store.Put(42, text("int-key"));
        store.Put(std::string("42"), text("string-key"));
        store.Put(binary_42, text("binary-key"));
        store.Put(binary_raw, text("raw-binary"));
    }

    KVStore reopened(db_path);
    const auto int_value = reopened.Get(42);
    const auto string_value = reopened.Get(std::string("42"));
    const auto binary_value = reopened.Get(binary_42);
    const auto raw_binary_value = reopened.Get(binary_raw);
    require(int_value.has_value() && as_string(*int_value) == "int-key", "int namespace should be preserved");
    require(string_value.has_value() && as_string(*string_value) == "string-key", "string namespace should be preserved");
    require(binary_value.has_value() && as_string(*binary_value) == "binary-key", "binary namespace should be preserved");
    require(raw_binary_value.has_value() && as_string(*raw_binary_value) == "raw-binary",
            "binary keys should support embedded zero bytes");
}

void test_write_batch_persists_mixed_key_types() {
    TestDir dir("batch_mixed_keys");
    const std::string db_path = dir.file("store.dat");
    const std::vector<uint8_t> binary_key = {0x00, 0x01, 0x02};

    {
        KVStore store(db_path);
        store.WriteBatch({
            BatchWriteOperation::PutInt(42, text("int-key")),
            BatchWriteOperation::Put("42", text("string-key")),
            BatchWriteOperation::Put("alpha", text("word")),
            BatchWriteOperation::PutBinary(binary_key, text("binary-word")),
        });
    }

    KVStore reopened(db_path);
    const auto int_value = reopened.Get(42);
    const auto string_value = reopened.Get(std::string("42"));
    const auto alpha_value = reopened.Get(std::string("alpha"));
    const auto binary_value = reopened.Get(binary_key);
    require(int_value.has_value(), "batch int key should persist");
    require(string_value.has_value(), "batch string key should persist");
    require(alpha_value.has_value(), "batch string key should persist");
    require(binary_value.has_value(), "batch binary key should persist");
    require(as_string(*int_value) == "int-key", "batch int key should keep its namespace");
    require(as_string(*string_value) == "string-key", "batch string key should not collide with int namespace");
    require(as_string(*alpha_value) == "word", "batch string key should round-trip");
    require(as_string(*binary_value) == "binary-word", "batch binary key should round-trip");
}

void test_write_batch_mixes_put_and_delete() {
    TestDir dir("batch_put_delete");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    store.Put(1, text("old"));
    store.Put(9, text("remove-me"));
    store.Put(std::string("alpha"), text("old-alpha"));

    store.WriteBatch({
        BatchWriteOperation::PutInt(1, text("new")),
        BatchWriteOperation::Delete("alpha"),
        BatchWriteOperation::Put("beta", text("beta-value")),
        BatchWriteOperation::DeleteInt(9),
    });

    const auto one = store.Get(1);
    const auto nine = store.Get(9);
    const auto alpha = store.Get(std::string("alpha"));
    const auto beta = store.Get(std::string("beta"));
    require(one.has_value() && as_string(*one) == "new", "batch put should replace existing int values");
    require(!nine.has_value(), "batch delete should remove int keys");
    require(!alpha.has_value(), "batch delete should remove string keys");
    require(beta.has_value() && as_string(*beta) == "beta-value", "batch put should insert new string keys");
}

void test_write_batch_preserves_operation_order() {
    TestDir dir("batch_order");
    const std::string db_path = dir.file("store.dat");

    {
        KVStore store(db_path);
        store.WriteBatch({
            BatchWriteOperation::PutInt(7, text("first")),
            BatchWriteOperation::DeleteInt(7),
            BatchWriteOperation::PutInt(7, text("second")),
            BatchWriteOperation::Put("gamma", text("a")),
            BatchWriteOperation::Put("gamma", text("b")),
        });
    }

    KVStore reopened(db_path);
    const auto seven = reopened.Get(7);
    const auto gamma = reopened.Get(std::string("gamma"));
    require(seven.has_value() && as_string(*seven) == "second", "batch operations should apply in-order for int keys");
    require(gamma.has_value() && as_string(*gamma) == "b", "batch operations should apply in-order for string keys");
}

void test_put_copies_input_value() {
    TestDir dir("copy");
    const std::string db_path = dir.file("store.dat");
    KVStore store(db_path);

    Value original = text("alpha");
    store.Put(7, original);
    original.bytes[0] = static_cast<uint8_t>('x');

    const auto stored = store.Get(7);
    require(stored.has_value(), "copied value should exist");
    require(as_string(*stored) == "alpha", "stored value should not alias caller buffer");
}

}  // namespace

void register_basic_kv_tests(TestCases& tests) {
    tests.push_back({"basic persistence", test_basic_persistence});
    tests.push_back({"string keys do not collide with int keys", test_string_keys_do_not_collide_with_int_keys});
    tests.push_back({"string scan returns sorted range", test_string_scan_returns_sorted_range});
    tests.push_back({"binary keys do not collide with other namespaces", test_binary_keys_do_not_collide_with_other_namespaces});
    tests.push_back({"batch write persists mixed key types", test_write_batch_persists_mixed_key_types});
    tests.push_back({"batch write mixes put and delete", test_write_batch_mixes_put_and_delete});
    tests.push_back({"batch write preserves operation order", test_write_batch_preserves_operation_order});
    tests.push_back({"put copies input value", test_put_copies_input_value});
}

}  // namespace kvstore::tests::integration
