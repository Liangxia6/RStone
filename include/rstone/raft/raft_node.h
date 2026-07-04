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

  RequestVoteResponse HandleRequestVote(const RequestVoteRequest& request);
  AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& request);

  Status Propose(EntryType type, const std::string& command, LogEntry* entry);
  void SetCommitIndex(LogIndex commit_index);
  std::vector<LogEntry> TakeCommittedEntries();
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
