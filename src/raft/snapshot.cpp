#include "rstone/raft/snapshot.h"

#include <filesystem>
#include <fstream>

#include "rstone/storage/file_kv_engine.h"

namespace rstone {

Status Snapshotter::Create(const KvEngine& engine, const SnapshotMeta& meta,
                           const std::string& snapshot_path) const {
  std::error_code ec;
  std::filesystem::create_directories(std::filesystem::path(snapshot_path).parent_path(), ec);
  if (ec) {
    return Status::IoError("failed to create snapshot directory: " + ec.message());
  }

  std::ofstream output(snapshot_path);
  if (!output) {
    return Status::IoError("failed to create snapshot: " + snapshot_path);
  }
  output << "META " << meta.region_id << ' ' << meta.last_included_index << ' '
         << meta.last_included_term << ' ' << HexEncode(meta.prefix) << '\n';
  for (const auto& [key, value] : engine.ScanPrefix(meta.prefix)) {
    output << "KV " << HexEncode(key) << ' ' << HexEncode(value) << '\n';
  }
  if (!output) {
    return Status::IoError("failed to write snapshot: " + snapshot_path);
  }
  return Status::Ok();
}

Status Snapshotter::Restore(KvEngine* engine, const std::string& snapshot_path,
                            SnapshotMeta* meta) const {
  if (engine == nullptr) {
    return Status::InvalidArgument("engine must not be null");
  }

  std::ifstream input(snapshot_path);
  if (!input) {
    return Status::IoError("failed to open snapshot: " + snapshot_path);
  }

  std::string record_type;
  std::vector<KvMutation> mutations;
  while (input >> record_type) {
    if (record_type == "META") {
      std::string prefix_hex;
      SnapshotMeta parsed;
      input >> parsed.region_id >> parsed.last_included_index >> parsed.last_included_term >>
          prefix_hex;
      auto status = HexDecode(prefix_hex, &parsed.prefix);
      if (!status.ok()) {
        return status;
      }
      if (meta != nullptr) {
        *meta = parsed;
      }
    } else if (record_type == "KV") {
      std::string key_hex;
      std::string value_hex;
      input >> key_hex >> value_hex;
      std::string key;
      std::string value;
      auto status = HexDecode(key_hex, &key);
      if (!status.ok()) {
        return status;
      }
      status = HexDecode(value_hex, &value);
      if (!status.ok()) {
        return status;
      }
      mutations.push_back(KvMutation{KvMutationType::kPut, key, value});
    } else {
      return Status::InvalidArgument("unknown snapshot record type: " + record_type);
    }
  }
  return engine->WriteBatch(mutations);
}

}  // namespace rstone
