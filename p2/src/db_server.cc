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
#include <shared_mutex>
#include <vector>
#include <cstddef>
#include <unordered_map>
#include <random>
#include <thread>
#include <unordered_map>
#include <ctime>
#include <unistd.h>
#include <utime.h>
#include <fstream>
#include <sstream>
#include <grpcpp/grpcpp.h>

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

#include "timer.hh"
#include "leveldb/db.h"

using util::Timer;
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

// Move to file with server configs and read from there upon start
#define SERVER1 "0.0.0.0:50053"


enum State {FOLLOWER, CANDIDATE, LEADER};

// ***************************** Global Variables ****************************
int serverID;
int leaderID;

State currState;
int commitIndex = -1;
int lastApplied = -1;
Timer electionTimer(1);
Timer heartbeatTimer(1);

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

void setCurrState(State cs)
{
  currState = cs;
}

void RunServer(string server_address)
{
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
  if(argc != 3) {
    printf("Usage: ./db_server <serverID> <server address>\n");
    return 0;
  }

  leveldb::DB *db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, "/tmp/distributorsdb", &db);
  if (!status.ok()) std::cerr << status.ToString() << endl;
  assert(status.ok());

  string key = "name";
  string value = "navani";

  // write
  status = db->Put(leveldb::WriteOptions(), key, value);
  assert(status.ok());

  // read
  status = db->Get(leveldb::ReadOptions(), key, &value);
  assert(status.ok());

  std::cout << value << endl;

  // delete
  // status = db->Delete(leveldb::WriteOptions(), key);
  // assert(status.ok());

  status = db->Get(leveldb::ReadOptions(), key, &value);
  if (!status.ok()) {
      std::cerr << key << "    " << status.ToString() << endl;
  } else {
      std::cout << key << "===" << value << endl;
  }
  //close
  // delete db;
  serverID = atoi(argv[1]);
  setCurrState(FOLLOWER);
  electionTimer.set_running(false);
  heartbeatTimer.set_running(false);

  RunServer(argv[2]);
  return 0;
}
