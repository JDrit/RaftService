
namespace java edu.rit.csh.scaladb.raft.server.internal
#@namespace scala edu.rit.csh.scaladb.raft.server.internal

typedef i32 Term
typedef string ServerId

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

/**
 * Log entries contain either regular client commands or new configuration changes. This
 * is used to distinguish them from each other.
 */
enum EntryType {
    CONFIGURATION,
    COMMAND
}

struct Entry {
    1: required Term term;
    2: required i32 index;
    3: required EntryType type;
    4: optional string command;
    5: optional list<string> newConfiguration;
}

struct AppendEntries {
    1: required Term term;            // leader’s term
    2: required ServerId leaderId;    // so follower can redirect clients
    3: required i32 prevLogIndex;     // index of log entry immediately preceding new ones
    4: required Term prevLogTerm;     // term of prevLogIndex entry
    5: required list<Entry> entires;  // log entries to store (empty for heartbeat; may send more than one for efficiency)
    6: required i32 leaderCommit;     // leader’s commitIndex
}

struct AppendEntriesResponse {
    1: required Term term;     // currentTerm, for leader to update itself
    2: required bool success;  // true if follower contained entry matching prevLogIndex and prevLogTerm
}

/**
 * Service for all calls by the administrator to the service to get the system's
 * stats and to do configuration management.
 */
service AdminService {
    bool changeConfig(1: list<string> servers)
    string stats();
}

/**
 * Service calls used for internal service communication
 */
service internalService extends AdminService {
    VoteResponse vote(1: RequestVote request);
    AppendEntriesResponse append(1: AppendEntries entries);
}