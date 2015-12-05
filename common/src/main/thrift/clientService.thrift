
namespace java edu.rit.csh.scaladb.raft.client
#@namespace scala edu.rit.csh.scaladb.raft.client

/**
 * Each client needs to attach unique serial numbers to each request so to make sure that
 * the request does not get processed more than once
 */
typedef i32 ID

/**
 * The unique ID of the client. This needs to be unique since it is used to distinguish clients
  * on the server end
 */
typedef string clientID

struct GetRequest {
    1: required clientID clientId;
    2: required ID commandId;
    3: required string key;
}

struct GetResponse {
    1: optional string value;
}

struct PutRequest {
    1: required clientID clientId;
    2: required ID commandId;
    3: required string key;
    4: required string value;
}

struct PutResponse {
    1: required bool updated;
}

struct CASRequest {
    1: required clientID clientId;
    2: required ID commandId;
    3: required string key;
    4: required string curValue;
    5: required string newValue;
}

struct CASResponse {
    1: required bool replaced;
}

struct DeleteRequest {
    1: required clientID clientId;
    2: required ID commandId;
    3: required string key;
}

struct DeleteResponse {
    1: required bool deleted;
}

exception NotLeader {
    1: required string leaderAddress;
}

service ClientOperations {
    GetResponse get(1: GetRequest get) throws (1: NotLeader notLeader);
    PutResponse put(1: PutRequest put) throws (1: NotLeader notLeader);
    DeleteResponse delete(1: DeleteRequest delete) throws (1: NotLeader notLeader)
    CASResponse cas(1: CASRequest cas) throws (1: NotLeader notLeader);
}