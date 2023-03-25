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

#include "afs.grpc.pb.h"
#include "afs_client.hh"

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
using afs::DirectoryEntry;
using afs::PingMessage;


FileSystemClient::FileSystemClient(std::shared_ptr<Channel> channel)
    : stub_(FileSystemService::NewStub(channel))
{
    open_map.clear();
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
