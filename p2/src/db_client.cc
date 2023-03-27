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

FileSystemClient::FileSystemClient(std::shared_ptr<Channel> channel)
    : stub_(RaftServer::NewStub(channel))
{
    std::cout << "-------------- Helloo --------------" << std::endl;
}

int FileSystemClient::Ping(int* round_trip_time)
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


int main()
{

	// init grpc connection
	string target_address(SERVER_ADDR);
    FileSystemClient* client;
    client = new FileSystemClient(grpc::CreateChannel(target_address,
                                grpc::InsecureChannelCredentials()));
	printf("initgRPC: Client is contacting server: %s\n", SERVER_ADDR);
	
	int ping_time;
	client->Ping(&ping_time);
    return 0;
}