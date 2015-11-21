
namespace java edu.rit.csh.scaladb.raft.client
#@namespace scala edu.rit.csh.scaladb.raft.client

typedef string ID

struct GetRequest {
    1: required ID commandId;
    2: required string key;
}

struct GetResponse {
    1: optional string value;
}

struct PutRequest {
    1: required ID commandId;
    2: required string key;
    3: required string value;
}

struct PutResponse {
    1: required bool updated;
}

struct CASRequest {
    1: required ID commandId;
    2: required string key;
    3: required string curValue;
    4: required string newValue;
}

struct CASResponse {
    1: required bool replaced;
}

struct DeleteRequest {
    1: required ID commandId;
    2: required string key;
}

struct DeleteResponse {
    1: required bool deleted;
}

service ClientOperations {
    GetResponse get(1: GetRequest get);
    PutResponse put(1: PutRequest put);
    CASResponse cas(1: CASRequest cas);
}