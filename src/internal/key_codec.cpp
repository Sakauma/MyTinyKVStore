#include "key_codec.h"

#include "kvstore.h"

namespace kvstore::internal {

std::string encode_int_key(int32_t key) {
    std::string encoded(1, kIntKeyTag);
    encoded.push_back(static_cast<char>((static_cast<uint32_t>(key) >> 24) & 0xFF));
    encoded.push_back(static_cast<char>((static_cast<uint32_t>(key) >> 16) & 0xFF));
    encoded.push_back(static_cast<char>((static_cast<uint32_t>(key) >> 8) & 0xFF));
    encoded.push_back(static_cast<char>(static_cast<uint32_t>(key) & 0xFF));
    return encoded;
}

std::string encode_string_key(const std::string& key) {
    std::string encoded(1, kStringKeyTag);
    encoded += key;
    return encoded;
}

std::string encode_binary_key(const std::vector<uint8_t>& key) {
    std::string encoded(1, kBinaryKeyTag);
    if (!key.empty()) {
        encoded.append(reinterpret_cast<const char*>(key.data()), key.size());
    }
    return encoded;
}

bool is_string_key(const std::string& key) {
    return !key.empty() && key.front() == kStringKeyTag;
}

std::string decode_string_key(const std::string& key) {
    if (!is_string_key(key)) {
        throw KVStoreError("Attempted to decode non-string key");
    }
    return key.substr(1);
}

}  // namespace kvstore::internal
