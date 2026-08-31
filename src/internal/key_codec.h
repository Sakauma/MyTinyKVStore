#ifndef KVSTORE_INTERNAL_KEY_CODEC_H
#define KVSTORE_INTERNAL_KEY_CODEC_H

#include <cstdint>
#include <string>
#include <vector>

namespace kvstore::internal {

inline constexpr char kIntKeyTag = '\x01';
inline constexpr char kStringKeyTag = '\x02';
inline constexpr char kBinaryKeyTag = '\x03';

std::string encode_int_key(int32_t key);
std::string encode_string_key(const std::string& key);
std::string encode_binary_key(const std::vector<uint8_t>& key);
bool is_string_key(const std::string& key);
std::string decode_string_key(const std::string& key);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_KEY_CODEC_H
