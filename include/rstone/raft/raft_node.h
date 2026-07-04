#pragma once

#include <optional>
#include <string>
#include <vector>

#include "rstone/common/status.h"
#include "rstone/common/types.h"

namespace rstone {

enum class RaftRole {
  kFollower,
  kCandidate,
  kLeader,
};

enum class EntryType {
  kNormal,
  kConfigChange,
  kSplit,
  kNoop,
};

struct LogEntry {
  RegionId region_id = 0;
  LogIndex index = 0;
  Term term = 0;
  EntryType type = EntryType::kNormal;
  std::string command;
};

struct RequestVoteRequest {
  RegionId region_id = 0;
  Term term = 0;
  PeerId candidate_id = 0;
  LogIndex last_log_index = 0;
  Term last_log_term = 0;
};

struct RequestVoteResponse {
  Term term = 0;
  bool vote_granted = false;
};

struct AppendEntriesRequest {
  RegionId region_id = 0;
  Term term = 0;
  PeerId leader_id = 0;
  LogIndex prev_log_index = 0;
  Term prev_log_term = 0;
  std::vector<LogEntry> entries;
  LogIndex leader_commit = 0;
};

struct AppendEntriesResponse {
  Term term = 0;
  bool success = false;
  LogIndex match_index = 0;
};

class RaftNode {
 public:
  RaftNode(RegionId region_id, PeerId self_id, std::vector<PeerId> peers);

  RegionId region_id() const { return region_id_; }
  PeerId self_id() const { return self_id_; }
  RaftRole role() const { return role_; }
  Term current_term() const { return current_term_; }
  std::optional<PeerId> voted_for() const { return voted_for_; }
  LogIndex commit_index() const { return commit_index_; }
  LogIndex last_applied() const { return last_applied_; }
  LogIndex last_log_index() const;
  Term last_log_term() const;
  const std::vector<LogEntry>& log() const { return log_; }

  void BecomeFollower(Term term);
  void BecomeCandidate();
  void BecomeLeader();

  // 处理候选人的投票请求；这里只判断任期、是否已投票以及候选日志是否足够新。
  RequestVoteResponse HandleRequestVote(const RequestVoteRequest& request);
  // 处理 Leader 的日志复制/心跳请求；日志匹配后才追加并推进 commit index。
  AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& request);

  // Leader 接收到写请求后先生成本地日志，真正提交要等上层复制到多数派。
  Status Propose(EntryType type, const std::string& command, LogEntry* entry);
  void SetCommitIndex(LogIndex commit_index);
  // 取出已经提交但尚未应用到状态机的日志，上层负责把 command 应用到 KV Engine。
  std::vector<LogEntry> TakeCommittedEntries();
  // 节点重启后从 RaftStorage 恢复任期、投票、commit/apply 位置和日志。
  Status Restore(Term current_term, std::optional<PeerId> voted_for,
                 LogIndex commit_index, LogIndex last_applied,
                 std::vector<LogEntry> log);

 private:
  bool IsCandidateLogUpToDate(LogIndex last_log_index, Term last_log_term) const;
  bool HasLogAt(LogIndex index, Term term) const;
  void DeleteFrom(LogIndex index);

  RegionId region_id_ = 0;
  PeerId self_id_ = 0;
  std::vector<PeerId> peers_;
  RaftRole role_ = RaftRole::kFollower;
  Term current_term_ = 0;
  std::optional<PeerId> voted_for_;
  LogIndex commit_index_ = 0;
  LogIndex last_applied_ = 0;
  std::vector<LogEntry> log_;
};

std::string RaftRoleName(RaftRole role);

}  // namespace rstone
