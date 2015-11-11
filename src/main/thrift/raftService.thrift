
namespace java edu.rit.csh.scaladb.raft
#@namespace scala edu.rit.csh.scaladb.raft

struct RequestVote {
    1: required i32 term;           // candidate’s term
    2: required string candidateId; // candidate requesting vote
    3: required i32 lastLogIndex;   // index of candidate’s last log entry
    4: required i32 lastLogTerm;    // term of candidate’s last log entry
}

struct VoteResponse {
    1: required i32 term;          // currentTerm, for candidate to update itself
    2: required bool voteGranted;  // true means candidate received vote
}

struct Entry {
    1: required i32 term;
    2: required i32 index;
    3: required string command;
}

struct AppendEntries {
    1: required i32 term;             // leader’s term
    2: required string leaderId;      // so follower can redirect clients
    3: required i32 prevLogIndex;     // index of log entry immediately preceding new ones
    4: required i32 prevLogTerm;      // term of prevLogIndex entry
    5: required list<Entry> entires;  // log entries to store (empty for heartbeat; may send more than one for efficiency)
    6: required i32 leaderCommit;     // leader’s commitIndex
}

struct AppendResponse {
    1: required i32 term;      // currentTerm, for leader to update itself
    2: required bool success;  // true if follower contained entry matching prevLogIndex and prevLogTerm
}

service RaftService {
    VoteResponse vote(1: RequestVote request);
    AppendResponse append(2: AppendEntries entries);
}