#include <grpcpp/grpcpp.h>
#include "db.grpc.pb.h"
//#include <unreliablefs_ops.h>

using namespace std;
using grpc::Channel;
using db::RaftServer;
using grpc::Status;



class FileSystemClient {
  public:
    FileSystemClient(std::shared_ptr<Channel> channel);
    int Ping(int * round_trip_time);

  private:
    std::unique_ptr<RaftServer::Stub> stub_;
    unordered_map<string, struct timespec> open_map;
};
