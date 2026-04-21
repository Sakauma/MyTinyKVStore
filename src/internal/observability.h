#ifndef KVSTORE_INTERNAL_OBSERVABILITY_H
#define KVSTORE_INTERNAL_OBSERVABILITY_H

#include "kvstore.h"

#include <string>

namespace kvstore::internal {

std::string metrics_to_json(const KVStoreMetrics& metrics);
KVStoreOptions recommended_options(KVStoreProfile profile);
std::string options_to_json(const KVStoreOptions& options);

}  // namespace kvstore::internal

#endif  // KVSTORE_INTERNAL_OBSERVABILITY_H
