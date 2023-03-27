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
#include "db.grpc.pb.h"
#else
#include "db.grpc.pb.h"
#endif

#define CHUNK_SIZE 4096

using db::RaftServer;
using db::PingMessage;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
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


// ***************************** RaftClient Code *****************************

class RaftClient {
  public:
    RaftClient(std::shared_ptr<Channel> channel);
    int PingOtherServers();

  private:
    std::unique_ptr<RaftServer::Stub> stub_;
};

RaftClient::RaftClient(std::shared_ptr<Channel> channel)
    : stub_(RaftServer::NewStub(channel)){}


int RaftClient::PingOtherServers(){
    printf("Hi in PingOtherServers\n");
    string target_address(SERVER1);
    std::unique_ptr<RaftServer::Stub> server1_stub;

    server1_stub = RaftServer::NewStub(grpc::CreateChannel(SERVER1, grpc::InsecureChannelCredentials()));
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

// ***************************** RaftServer Code *****************************

class dbImpl final : public RaftServer::Service
{

public:
    RaftClient* raftClient;
    explicit dbImpl() {}

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
    std::string server_address("0.0.0.0:50053");
    dbImpl service;

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
