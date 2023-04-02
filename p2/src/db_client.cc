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

#include "db.grpc.pb.h"
#include "db_client.hh"
#include "config.hh"

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
using db::PingMessage;

#define SERVER_ADDR           "0.0.0.0:50052"
typedef std::unique_ptr<RaftServer::Stub> ServerStub;

int leaderID = 0; // hardcoded leaderID for now, should remove.
DbClient::DbClient(std::shared_ptr<Channel> channel)
    : stub_(RaftServer::NewStub(channel))
{
    std::cout << "-------------- Helloo --------------" << std::endl;
}
DbClient::DbClient()
{
    std::cout << "-------------- Helloo --------------" << std::endl;
}

/********************* Helper Functions **************************************/
ServerStub initConnection(int id){
    ServerStub stub;
    stub = RaftServer::NewStub(grpc::CreateChannel(SERVER1, grpc::InsecureChannelCredentials()));

	printf("initgRPC: Client is contacting server: %s\n", SERVER1);
    return stub;
}

int DbClient::Ping(int* round_trip_time)
{
    auto start = std::chrono::steady_clock::now();

    PingMessage request;
    PingMessage reply;
    Status status;

    ClientContext context;
    status = stub_->Ping(&context, request, &reply);

    if (status.ok()) {
        auto end = std::chrono::steady_clock::now();
        std::chrono::nanoseconds ns = end - start;
        *round_trip_time = ns.count();
        return 0;
    }
    else {
        return -1;
    }
}

int DbClient::Get(string key, string* value) {
    printf("[Get]: Invoked;\n");

    GetRequest req;
    GetResponse res;
    Status status;

    ClientContext context;
    req.set_key(key);
    res.Clear();

    ServerStub stub = initConnection(leaderID);
    status = stub->Get(&context, req, &res);

    if (status.ok()) {
        int server_errno = res.db_errno();
        if (server_errno) {
            printf("[Get]: Error %d on server.\n", server_errno);
            printf("[Get]: Function ended due to server failure.\n");
            errno = server_errno;
            return -1;
        }
        // value = &res.value();
        printf("%s\n", res.value().c_str());
        
        printf("[Get]: Function ended;\n");
        return 0;
    } else {
        errno = RPCstatusCode(status.error_code());
        printf("[Get]: RPC failure\n");
        return -1;
    }
}


int main(int argc, char **argv)
{
    string key, value;
    DbClient* client;
    client = new DbClient();
    if(argc == 2){
        key = argv[1];
        string val;
        client->Get(key, &val);
    }
    else if(argc == 3){
        key = argv[1];
        value = argv[2];
    }else{
        printf("Usage:\nTo get value from db:\n ./db_client <key> \nTo put key value pair in db:\n ./db_client <key> <value>\n");
        return 0;
    }

	// init grpc connection
    return 0;
}