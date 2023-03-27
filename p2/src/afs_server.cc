#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <filesystem>
#include <shared_mutex>
#include <vector>
#include <algorithm>
#include <cstddef>
#include <unordered_map>

#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <unordered_map>
#include <grpcpp/grpcpp.h>
#include <string>
#include <chrono>
#include <ctime>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <utime.h>
#include <fstream>
#include <sstream>


#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#ifdef BAZEL_BUILD
#include "afs.grpc.pb.h"
#else
#include "afs.grpc.pb.h"
#endif

#define CHUNK_SIZE 4096


using afs::PingMessage;
using afs::FileSystemService;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::StatusCode;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::StatusCode;
using std::cout;
using std::endl;
using std::string;

#define SERVER1 "0.0.0.0:50053"

class RaftClient {
  public:
    RaftClient(std::shared_ptr<Channel> channel);
    int Ping(int * round_trip_time);
    int PingFollower(int * round_trip_time);

    int PingOtherServers();

  private:
    std::unique_ptr<FileSystemService::Stub> stub_;
};

RaftClient::RaftClient(std::shared_ptr<Channel> channel)
    : stub_(FileSystemService::NewStub(channel)){}


int RaftClient::PingOtherServers(){
    printf("Hi in PingOtherServers\n");
    string target_address(SERVER1);
    std::unique_ptr<FileSystemService::Stub> server1_stub;

    server1_stub = FileSystemService::NewStub(grpc::CreateChannel(SERVER1, grpc::InsecureChannelCredentials()));
	printf("initgRPC: Client is contacting server: %s\n", SERVER1);
	
	int ping_time;
    PingMessage request;
    PingMessage reply;
    Status status;

    ClientContext context;
    status = server1_stub->PingFollower(&context, request, &reply);
    printf("%d\n", status.ok());
    if (status.ok())return 0;
    else return -1;
}



class AFSImpl final : public FileSystemService::Service
{

public:
    RaftClient* raftClient;
    explicit AFSImpl() {}

    Status Ping(ServerContext *context, const PingMessage *req, PingMessage *resp) override
    {
        printf("Got Pinged\n");
        raftClient->PingOtherServers();
        return Status::OK;
    }

    Status PingFollower(ServerContext *context, const PingMessage *req, PingMessage *resp) override
    {
        printf("Got Pinged by other node's client \n");
        return Status::OK;
    }
};


void RunServer()
{
    std::string server_address("0.0.0.0:50052");
    AFSImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char **argv)
{

    RunServer();
    return 0;
}
