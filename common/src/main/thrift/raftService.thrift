
namespace java edu.rit.csh.scaladb.raft
#@namespace scala edu.rit.csh.scaladb.raft

include "admin.thrift"

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

enum Configuration {
    C_NEW_OLD,
    C_NEW
}

struct Entry {
    1: required Term term;
    2: required i32 index;
    3: required EntryType type;
    4: optional binary command;
    5: optional list<admin.Server> newConfiguration;
    6: optional Configuration configurationType;
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
    3: optional i32 lastIndex; // the index of the last element in the node's log
}

/**
 * Service calls used for internal service communication
 */
service InternalService extends admin.AdminService {
    VoteResponse vote(1: RequestVote request);
    AppendEntriesResponse append(1: AppendEntries entries);
}