#include <grpcpp/grpcpp.h>
#include "afs.grpc.pb.h"
//#include <unreliablefs_ops.h>

using namespace std;
using grpc::Channel;
using afs::FileSystemService;
using grpc::Status;



class FileSystemClient {
  public:
    FileSystemClient(std::shared_ptr<Channel> channel);
    int Ping(int * round_trip_time);

  private:
    std::unique_ptr<FileSystemService::Stub> stub_;
    unordered_map<string, struct timespec> open_map;
};
