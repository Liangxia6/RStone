#include "rstone/raft/raft_node.h"

#include "test_assert.h"

namespace {

std::vector<rstone::PeerId> Peers() { return {1, 2, 3}; }

void TestRequestVote() {
  rstone::RaftNode node(1, 1, Peers());

  rstone::RequestVoteRequest request;
  request.region_id = 1;
  request.term = 1;
  request.candidate_id = 2;
  auto response = node.HandleRequestVote(request);
  RSTONE_ASSERT_TRUE(response.vote_granted);
  RSTONE_ASSERT_EQ(response.term, static_cast<rstone::Term>(1));
  RSTONE_ASSERT_TRUE(node.voted_for().has_value());
  RSTONE_ASSERT_EQ(*node.voted_for(), static_cast<rstone::PeerId>(2));

  request.candidate_id = 3;
  response = node.HandleRequestVote(request);
  RSTONE_ASSERT_TRUE(!response.vote_granted);
}

void TestAppendEntriesAndCommit() {
  rstone::RaftNode leader(1, 1, Peers());
  leader.BecomeCandidate();
  leader.BecomeLeader();

  rstone::LogEntry entry;
  RSTONE_ASSERT_TRUE(leader.Propose(rstone::EntryType::kNormal, "put a 1", &entry).ok());

  rstone::RaftNode follower(1, 2, Peers());
  rstone::AppendEntriesRequest append;
  append.region_id = 1;
  append.term = leader.current_term();
  append.leader_id = 1;
  append.prev_log_index = 0;
  append.prev_log_term = 0;
  append.entries.push_back(entry);
  append.leader_commit = 1;

  const auto response = follower.HandleAppendEntries(append);
  RSTONE_ASSERT_TRUE(response.success);
  RSTONE_ASSERT_EQ(follower.last_log_index(), static_cast<rstone::LogIndex>(1));
  RSTONE_ASSERT_EQ(follower.commit_index(), static_cast<rstone::LogIndex>(1));

  const auto committed = follower.TakeCommittedEntries();
  RSTONE_ASSERT_EQ(committed.size(), static_cast<std::size_t>(1));
  RSTONE_ASSERT_EQ(committed.front().command, "put a 1");
}

void TestRestore() {
  std::vector<rstone::LogEntry> log;
  log.push_back({1, 1, 3, rstone::EntryType::kNormal, "one"});
  log.push_back({1, 2, 3, rstone::EntryType::kNormal, "two"});

  rstone::RaftNode node(1, 1, Peers());
  RSTONE_ASSERT_TRUE(node.Restore(3, rstone::PeerId{2}, 2, 1, log).ok());
  RSTONE_ASSERT_EQ(node.current_term(), static_cast<rstone::Term>(3));
  RSTONE_ASSERT_TRUE(node.voted_for().has_value());
  RSTONE_ASSERT_EQ(*node.voted_for(), static_cast<rstone::PeerId>(2));
  RSTONE_ASSERT_EQ(node.last_log_index(), static_cast<rstone::LogIndex>(2));

  const auto committed = node.TakeCommittedEntries();
  RSTONE_ASSERT_EQ(committed.size(), static_cast<std::size_t>(1));
  RSTONE_ASSERT_EQ(committed.front().command, "two");
}

}  // namespace

struct RaftNodeTestRunner {
  RaftNodeTestRunner() {
    TestRequestVote();
    TestAppendEntriesAndCommit();
    TestRestore();
  }
};

static RaftNodeTestRunner raft_node_test_runner;
