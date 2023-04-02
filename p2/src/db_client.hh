#include <grpcpp/grpcpp.h>
#include "db.grpc.pb.h"
//#include <unreliablefs_ops.h>

using namespace std;
using grpc::Channel;
using db::RaftServer;
using db::GetRequest;
using db::GetResponse;
using grpc::Status;
using grpc::StatusCode;



class DbClient {
  public:
    DbClient(std::shared_ptr<Channel> channel);
    DbClient();
    int Ping(int * round_trip_time);
    int Get(string key, string* value);
    
    

  private:
    std::unique_ptr<RaftServer::Stub> stub_;
    unordered_map<string, struct timespec> open_map;
};


int RPCstatusCode(grpc::StatusCode code)
{
    switch (code) {
    case StatusCode::OK:
        return 0;
    case StatusCode::INVALID_ARGUMENT:
        return EINVAL;
    case StatusCode::NOT_FOUND:
        return ENOENT;
    case StatusCode::DEADLINE_EXCEEDED:
    case StatusCode::UNAVAILABLE:
        return ETIMEDOUT;
    case StatusCode::ALREADY_EXISTS:
        return EEXIST;
    case StatusCode::PERMISSION_DENIED:
    case StatusCode::UNAUTHENTICATED:
        return EPERM;
    default:
        return EIO;
    }
}