
namespace java edu.rit.csh.scaladb.raft.admin
#@namespace scala edu.rit.csh.scaladb.raft.admin

struct Server {
    1: string raftUrl
    2: string clientUrl
}

/**
 * Service for all calls by the administrator to the service to get the system's
 * stats and to do configuration management.
 */
service AdminService {
    bool changeConfig(1: list<Server> servers)
    string stats();
}
