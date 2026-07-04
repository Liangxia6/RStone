#include "rstone/raft/raft_node.h"

#include <algorithm>
#include <utility>

namespace rstone {

RaftNode::RaftNode(RegionId region_id, PeerId self_id, std::vector<PeerId> peers)
    : region_id_(region_id), self_id_(self_id), peers_(std::move(peers)) {}

LogIndex RaftNode::last_log_index() const {
  if (log_.empty()) {
    return 0;
  }
  return log_.back().index;
}

Term RaftNode::last_log_term() const {
  if (log_.empty()) {
    return 0;
  }
  return log_.back().term;
}

void RaftNode::BecomeFollower(Term term) {
  role_ = RaftRole::kFollower;
  if (term > current_term_) {
    current_term_ = term;
    voted_for_.reset();
  }
}

void RaftNode::BecomeCandidate() {
  role_ = RaftRole::kCandidate;
  ++current_term_;
  voted_for_ = self_id_;
}

void RaftNode::BecomeLeader() {
  role_ = RaftRole::kLeader;
}

RequestVoteResponse RaftNode::HandleRequestVote(const RequestVoteRequest& request) {
  RequestVoteResponse response;

  // 旧任期的候选人不能获得投票；Raft 通过 term 单调递增避免旧 Leader 继续写入。
  if (request.term < current_term_) {
    response.term = current_term_;
    response.vote_granted = false;
    return response;
  }

  if (request.term > current_term_) {
    BecomeFollower(request.term);
  }

  // 一个任期内最多投一票，同时候选人的日志不能比本节点旧。
  const bool can_vote = !voted_for_.has_value() || *voted_for_ == request.candidate_id;
  const bool up_to_date =
      IsCandidateLogUpToDate(request.last_log_index, request.last_log_term);

  if (can_vote && up_to_date) {
    voted_for_ = request.candidate_id;
    response.vote_granted = true;
  }

  response.term = current_term_;
  return response;
}

AppendEntriesResponse RaftNode::HandleAppendEntries(const AppendEntriesRequest& request) {
  AppendEntriesResponse response;

  // Leader 任期落后时直接拒绝，调用方会据此停止旧 Leader 行为。
  if (request.term < current_term_) {
    response.term = current_term_;
    response.success = false;
    response.match_index = last_log_index();
    return response;
  }

  if (request.term > current_term_ || role_ != RaftRole::kFollower) {
    BecomeFollower(request.term);
  }

  response.term = current_term_;

  if (!HasLogAt(request.prev_log_index, request.prev_log_term)) {
    response.success = false;
    response.match_index = last_log_index();
    return response;
  }

  for (const auto& entry : request.entries) {
    if (entry.index == 0) {
      response.success = false;
      return response;
    }
    if (entry.index <= last_log_index()) {
      const auto existing = log_[entry.index - 1];
      if (existing.term != entry.term) {
        // 同一 index 任期不同，说明出现日志冲突；删除冲突点及其之后的日志。
        DeleteFrom(entry.index);
        log_.push_back(entry);
      }
    } else {
      log_.push_back(entry);
    }
  }

  if (request.leader_commit > commit_index_) {
    commit_index_ = std::min(request.leader_commit, last_log_index());
  }

  response.success = true;
  response.match_index = last_log_index();
  return response;
}

Status RaftNode::Propose(EntryType type, const std::string& command, LogEntry* entry) {
  if (role_ != RaftRole::kLeader) {
    return {ErrorCode::kNotLeader, "proposal must be sent to leader"};
  }
  LogEntry new_entry;
  new_entry.region_id = region_id_;
  new_entry.index = last_log_index() + 1;
  new_entry.term = current_term_;
  new_entry.type = type;
  new_entry.command = command;
  log_.push_back(new_entry);
  if (entry != nullptr) {
    *entry = new_entry;
  }
  return Status::Ok();
}

void RaftNode::SetCommitIndex(LogIndex commit_index) {
  commit_index_ = std::min(commit_index, last_log_index());
}

std::vector<LogEntry> RaftNode::TakeCommittedEntries() {
  std::vector<LogEntry> entries;
  // last_applied 只向前推进，保证每条已提交日志最多应用一次。
  while (last_applied_ < commit_index_) {
    ++last_applied_;
    entries.push_back(log_[last_applied_ - 1]);
  }
  return entries;
}

Status RaftNode::Restore(Term current_term, std::optional<PeerId> voted_for,
                         LogIndex commit_index, LogIndex last_applied,
                         std::vector<LogEntry> log) {
  for (std::size_t i = 0; i < log.size(); ++i) {
    if (log[i].region_id != region_id_) {
      return Status::InvalidArgument("restored log belongs to another region");
    }
    if (log[i].index != i + 1) {
      return Status::InvalidArgument("restored log index is not contiguous");
    }
  }
  if (commit_index > log.size()) {
    return Status::InvalidArgument("restored commit index is past log end");
  }
  if (last_applied > commit_index) {
    return Status::InvalidArgument("restored last_applied is past commit index");
  }

  role_ = RaftRole::kFollower;
  current_term_ = current_term;
  voted_for_ = voted_for;
  commit_index_ = commit_index;
  last_applied_ = last_applied;
  log_ = std::move(log);
  return Status::Ok();
}

bool RaftNode::IsCandidateLogUpToDate(LogIndex last_log_index, Term last_log_term) const {
  const Term local_last_log_term = this->last_log_term();
  if (last_log_term != local_last_log_term) {
    return last_log_term > local_last_log_term;
  }
  return last_log_index >= this->last_log_index();
}

bool RaftNode::HasLogAt(LogIndex index, Term term) const {
  if (index == 0) {
    return term == 0;
  }
  if (index > log_.size()) {
    return false;
  }
  return log_[index - 1].term == term;
}

void RaftNode::DeleteFrom(LogIndex index) {
  if (index == 0 || index > log_.size() + 1) {
    return;
  }
  log_.resize(index - 1);
  if (commit_index_ >= index) {
    commit_index_ = index - 1;
  }
  if (last_applied_ >= index) {
    last_applied_ = index - 1;
  }
}

std::string RaftRoleName(RaftRole role) {
  switch (role) {
    case RaftRole::kFollower:
      return "Follower";
    case RaftRole::kCandidate:
      return "Candidate";
    case RaftRole::kLeader:
      return "Leader";
  }
  return "Unknown";
}

}  // namespace rstone
