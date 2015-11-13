
namespace java edu.rit.csh.scaladb.raft
#@namespace scala edu.rit.csh.scaladb.raft

typedef i32 Term
typedef i32 ServerId

struct RequestVote {
    1: required Term term;            // candidate’s term
    2: required ServerId candidateId; // candidate requesting vote
    3: required i32 lastLogIndex;     // index of candidate’s last log entry
    4: required i32 lastLogTerm;      // term of candidate’s last log entry
}

struct VoteResponse {
    1: required Term term;          // currentTerm, for candidate to update itself
    2: required bool voteGranted;   // true means candidate received vote
}

/*
 * Log entries contain either regular client commands or new configuration changes. This
 * is used to distinguish them from each other.
 */
enum EntryType {
    CONFIGURATION,
    COMMAND
}

struct Server {
    1: required ServerId id;
    2: required string address;
}

struct Configuration {
    1: required Term term;
    2: required ServerId leaderId;
    3: required list<Server> servers;
}

struct Entry {
    1: required Term term;
    2: required i32 index;
    3: required EntryType type;
    4: optional string command;
    5: optional Configuration configuration;
}

struct AppendEntries {
    1: required Term term;            // leader’s term
    2: required ServerId leaderId;    // so follower can redirect clients
    3: required i32 prevLogIndex;     // index of log entry immediately preceding new ones
    4: required Term prevLogTerm;     // term of prevLogIndex entry
    5: required list<Entry> entires;  // log entries to store (empty for heartbeat; may send more than one for efficiency)
    6: required i32 leaderCommit;     // leader’s commitIndex
}

struct AppendResponse {
    1: required Term term;     // currentTerm, for leader to update itself
    2: required bool success;  // true if follower contained entry matching prevLogIndex and prevLogTerm
}

service RaftService {
    VoteResponse vote(1: RequestVote request);
    AppendResponse append(2: AppendEntries entries);
}