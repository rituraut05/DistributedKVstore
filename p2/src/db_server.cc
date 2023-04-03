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

#include "config.hh"
#include "timer.hh"
#include "leveldb/db.h"

using util::Timer;
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
using std::stoi;
using std::to_string;
using std::thread;
using std::vector;
using std::default_random_engine;
using std::uniform_int_distribution;
using std::chrono::system_clock;

using db::RaftServer;
using db::PingMessage;
using db::GetRequest;
using db::GetResponse;
using db::PutRequest;
using db::PutResponse;

#define MIN_ELECTION_TIMEOUT   150
#define MAX_ELECTION_TIMEOUT   300
#define HEARTBEAT_TIMEOUT      70

// Move to file with server configs and read from there upon start



// ***************************** State enum **********************************
enum State {FOLLOWER, CANDIDATE, LEADER};
string stateNames[3] = {"FOLLOWER", "CANDIDATE", "LEADER"};

// ***************************** Log class ***********************************

vector<string> split(string str, char delim) {
  vector<string> strs;
  string temp = "";

  for(int i=0; i<str.length(); i++) {
    if(str[i] == delim) {
      strs.push_back(temp);
      temp = "";
    } else {
      temp = temp + (str.c_str())[i];
    }
  }
  strs.push_back(temp);
  return strs;
}

class Log {
  public:
    int index;
    int term;
    string key;
    string value;

    Log() {}
    Log(int index, int term, string key, string value){
      this->index = index;
      this->term = term;
      this->key = key;
      this->value = value;
    }

    // log string is of the format- index;term;key;value
    Log(string logString) {
      vector<string> logParts = split(logString, ';');
      index = stoi(logParts[0]);
      term = stoi(logParts[1]);
      key = logParts[2];
      value = logParts[3];
    }

    string toString() {
      string logString = "";
      logString += to_string(index) + ";";
      logString += to_string(term) + ";";
      logString += key + ";";
      logString += value;
      return logString;
    }
};

// ***************************** Volatile Variables **************************
int serverID;
uint32_t leaderID = 0;

State currState;
int commitIndex = 0; // index starts from 1
int lastLogIndex = 0; // index starts from 1 
Timer electionTimer(1, MAX_ELECTION_TIMEOUT);
Timer heartbeatTimer(1, HEARTBEAT_TIMEOUT);
bool electionRunning = false;

/*
* logs are stored as key - value pairs in plogs with
* key = index and value = log.toString()
*/
leveldb::DB *plogs;
/*
* Stores other persistent variables ie
* currentTerm, lastApplied, votedFor 
*/
leveldb::DB *pmetadata;
/*
* Actual db service 
*/
leveldb::DB *replicateddb;

// *************************** Persistent Variables **************************
int currentTerm = 0; // Valid terms start from 1
int lastApplied = 0; // index starts from 1
int votedFor = -1; // -1 means did not vote for anyone in current term yet
vector<Log> logs; // purge old logs periodically as the class stores the index information
                  // purging is done so we do not run out of memory 

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

int RaftClient::PingOtherServers() {
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

class dbImpl final : public RaftServer::Service {

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

  Status Get(ServerContext *context, const GetRequest *req, GetResponse *resp) override
  {
    printf("Get request invoked on %s\n", stateNames[currState].c_str());
    if(currState!= LEADER){
      resp->set_value("");
      printf("Setting leaderID to %d\n", leaderID);
      resp->set_leaderid(leaderID);
      resp->set_db_errno(EPERM);

      printf("leaderID Set in response: %d\n",resp->leaderid());
      return Status::OK;
    }
    printf("Get invoked on server\n");
    string value;
    string key = req->key();
    leveldb::Status status = replicateddb->Get(leveldb::ReadOptions(), key, &value);
    printf("Value: %s\n", value.c_str());
    resp->set_value(value);
    resp->set_leaderid(leaderID);
    return Status::OK;
  }

  Status Put(ServerContext *context, const PutRequest *req, PutResponse *resp) override
  {
    printf("Put request invoked on %s\n", stateNames[currState].c_str());
    if(currState!= LEADER){
      printf("Setting leaderID to %d\n", leaderID);
      resp->set_leaderid(leaderID);
      resp->set_db_errno(EPERM);

      printf("leaderID Set in response: %d\n",resp->leaderid());
      return Status::OK;
    }

    string value = req->value();
    string key = req->key();

    Log logEntry;

    std::shared_mutex mutex;
    int lli = lastLogIndex;
    
    
    mutex.lock();
    logEntry.index = logs.back().index+1;
    logEntry.term = currentTerm;
    logEntry.key = key;
    logEntry.value = value;

    logs.push_back(logEntry);
    lli = logEntry.index;
    lastLogIndex = logEntry.index;
    mutex.unlock();

    while(commitIndex< lli){
      printf("Waiting for commitIndex: %d == lastLogIndex: %d\n", commitIndex, lli);
    }
    printf("[Put] Success: (%s, %s) log committed\n", key.c_str(), value.c_str());

    resp->set_leaderid(leaderID);
    return Status::OK;
  }
};

// ********************************* Functions ************************************

void setCurrState(State cs)
{
  currState = cs;
}

int getRandomTimeout() {
  unsigned seed = system_clock::now().time_since_epoch().count();
  default_random_engine generator(seed);
  uniform_int_distribution<int> distribution(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT);
  return distribution(generator);
}

// hardcoded for testing executeLog: REMOVE LATER
void testExecuteLog(){
  printf("in testExecuteLog\n");
  Log logEntryOne = Log(1, currentTerm, "key1", "value1");
  Log logEntryTwo = Log(1, currentTerm, "key2", "value2");
  Log logEntryThree = Log(1, currentTerm, "key3", "value3");
  logs.push_back(logEntryOne);
  logs.push_back(logEntryTwo);
  logs.push_back(logEntryThree); 
  commitIndex = 4;
  lastApplied = 0;
}

void executeLog() {
  testExecuteLog();
  int i=0;
  while(true) {
    i++;
    if(lastApplied < commitIndex) {
      lastApplied++;
      printf("[ExecuteLog]: Executing log from index: %d\n", lastApplied); 
      
      // find lastApplied index in logs
      auto i = logs.begin();
      for(; i!=logs.end(); i++){
        if(i->index == lastApplied){
          break;
        }
      }

      // This can happen when we have erased till min(matchIndex) which have not yet applied
      if(i== logs.end()){
        printf("[ExecuteLog]: Unable to find %d index in logs\n", lastApplied);
        continue;
        // TODO: if such case arises. Find in DB as well.
      }

      while(i->index < commitIndex) {
        // put to replicateddb
        leveldb::Status status = replicateddb->Put(leveldb::WriteOptions(), i->key, i->value);
        if(!status.ok()){
          lastApplied--;
          printf("[ExecuteLog]: Failure while put in replicated db %s\n", status.ToString().c_str());
          pmetadata->Put(leveldb::WriteOptions(), "lastApplied", to_string(lastApplied));
          break;
        }
        pmetadata->Put(leveldb::WriteOptions(), "lastApplied", to_string(lastApplied));
        lastApplied++;
        i++;
      }
    }else if(i%100==0){
      printf("[ExecuteLog]: Logs are up to date.\n");
    }
  }
}

void runElection() {
  electionRunning = true;
  currentTerm++;
  votedFor = serverID; 
  pmetadata->Put(leveldb::WriteOptions(), "currentTerm", to_string(currentTerm));
  pmetadata->Put(leveldb::WriteOptions(), "votedFor", to_string(votedFor));
  printf("[runElection] Running Election for term=%d\n", currentTerm);
  // RequestVotes, gather votes
  electionRunning = false;
}

void runElectionTimer() {
  votedFor = -1;
  pmetadata->Put(leveldb::WriteOptions(), "votedFor", to_string(votedFor));
  int timeout = getRandomTimeout();
  electionTimer.start(timeout);
  while(electionTimer.running() && 
    electionTimer.get_tick() < electionTimer._timeout) ; // spin
  printf("[runElectionTimer] Spun for %d ms before timing out in state %d for term %d\n", electionTimer.get_tick(), currState, currentTerm);
  runElection();
  setCurrState(CANDIDATE);
}

void runHeartbeatTimer() {
  heartbeatTimer.start(HEARTBEAT_TIMEOUT);
  while(heartbeatTimer.running() &&
    heartbeatTimer.get_tick() < heartbeatTimer._timeout) ; // spin
  // printf("[runHeartbeatTimer] Spun for %d ms before timing out to send heartbeat in term %d\n", heartbeatTimer.get_tick(), currentTerm);
  // sendHeartbeat();
  runHeartbeatTimer();
}

void runRaftServer() {
  thread electionTimerThread;
  thread heartbeatTimerThread;
  while(true) {
    if(currState == FOLLOWER) {
      if(heartbeatTimer.running()) {
        heartbeatTimer.set_running(false);
        heartbeatTimerThread.join();
      }
      if(!electionTimer.running()) {
        electionTimerThread.join();
        electionTimer.set_running(true);
        electionTimerThread = thread { runElectionTimer };
      }
    }
    if(currState == CANDIDATE) {
      if(heartbeatTimer.running()) {
        heartbeatTimer.set_running(false);
        heartbeatTimerThread.join();
      }
      if(!electionRunning) {
        electionTimerThread.join();
        electionTimer.set_running(true);
        electionTimerThread = thread { runElectionTimer };
      }
    }
    if(currState == LEADER) {
      if(electionTimer.running()) {
        electionTimer.set_running(false);
        electionTimerThread.join();
      }
      if(!heartbeatTimer.running()) {
        heartbeatTimer.set_running(true);
        heartbeatTimerThread = thread { runHeartbeatTimer };
      }
    }
  }
}
std::unique_ptr<Server> server;
void RunGrpcServer(string server_address) {
  dbImpl service;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  server = builder.BuildAndStart();
  std::cout << "Server listening on " << server_address << std::endl;
  std::unique_ptr<std::thread> worker(new std::thread([&]
  {
    server->Wait();
  }));

  if (worker->joinable())
  {
      worker->join();
  }
}

void openOrCreateDBs() {
  leveldb::Options options;
  options.create_if_missing = true;

  leveldb::Status plogs_status = leveldb::DB::Open(options, "/tmp/plogs" + to_string(serverID), &plogs);
  if (!plogs_status.ok()) std::cerr << plogs_status.ToString() << endl;
  assert(plogs_status.ok());

  leveldb::Status pmetadata_status = leveldb::DB::Open(options, "/tmp/pmetadata" + to_string(serverID), &pmetadata);
  if(!pmetadata_status.ok()) std::cerr << pmetadata_status.ToString() << endl;
  assert(pmetadata_status.ok());

  leveldb::Status replicateddb_status = leveldb::DB::Open(options, "/tmp/replicateddb" + to_string(serverID), &replicateddb);
  if(!replicateddb_status.ok()) std::cerr << replicateddb_status.ToString() << endl;
  assert(replicateddb_status.ok());
}

void initializePersistedValues() {
  string value;
  leveldb::Status currentTermStatus = pmetadata->Get(leveldb::ReadOptions(), "currentTerm", &value);
  if (!currentTermStatus.ok()) {
    std::cerr << "[initializePersistedValues] currentTerm" << ": Error: " << currentTermStatus.ToString() << endl;
  } else {
    currentTerm = stoi(value);
    printf("[initializePersistedValues] currentTerm = %d\n", currentTerm);
  }

  value = "";
  leveldb::Status lastAppliedStatus = pmetadata->Get(leveldb::ReadOptions(), "lastApplied", &value);
  if(!lastAppliedStatus.ok()) {
    std::cerr << "[initializePersistedValues] lastApplied" << ": Error: " << lastAppliedStatus.ToString() << endl;
  } else {
    lastApplied = stoi(value);
    printf("[initializePersistedValues] lastApplied = %d\n", lastApplied);
  }

  value = "";
  leveldb::Status votedForStatus = pmetadata->Get(leveldb::ReadOptions(), "votedFor", &value);
  if(!votedForStatus.ok()) {
    std::cerr << "[initializePersistedValues] votedFor" << ": Error: " << votedForStatus.ToString() << endl;
  } else {
    votedFor = stoi(value);
    printf("[initializePersistedValues] votedFor = %d\n", votedFor);
  }

  // get persisted logs from plogs and initialize
  int logidx = 1;
  while(true) {
    string logString = "";
    leveldb::Status logstatus = plogs->Get(leveldb::ReadOptions(), to_string(logidx), &logString);
    if(logstatus.ok()) {
      Log l(logString);
      logs.push_back(l);
      logidx++;
    } else {
      std::cerr << "[initializePersistedValues] logidx = " << logidx << ": Error: " << logstatus.ToString() << endl;
      break;
    }
  }
}

void testPut(){
  currentTerm++;
  Log logEntryOne = Log(0, currentTerm, "name", "Ritu"); // need something at index = 0?
  logs.push_back(logEntryOne); // hardcoded for testing put: REMOVE LATER
}

int main(int argc, char **argv) {
  if(argc != 3) {
    printf("Usage: ./db_server <serverID> <server address>\n");
    return 0;
  }

  // initialize values 
  serverID = atoi(argv[1]);

  if(serverID == leaderID){ // REMOVE LATER
    setCurrState(LEADER);
  }else{
    setCurrState(FOLLOWER);
  }
  
  electionTimer.set_running(false);
  heartbeatTimer.set_running(false); 

  // open leveldb DBs
  openOrCreateDBs();
  // read from persisted variables
  initializePersistedValues();

  // test log class - REMOVE LATER
  Log l("1;1;name;Snehal");
  cout << l.index << " " << l.term << " " << l.key << " " << l.value << endl;
  cout << l.toString() << endl;  

  //test get operation related operations - REMOVE LATER
  replicateddb->Put(leveldb::WriteOptions(), "name", "Ritu");

  testPut();

  // initialize channels to servers

  // Run threads
  thread logExecutor(executeLog);
  thread raftServer(runRaftServer);
  RunGrpcServer(argv[2]);

  logExecutor.join();
  raftServer.join();
  return 0;
}
