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
using db::RaftServer;
using db::PingMessage;
using db::AppendEntriesRequest;
using db::AppendEntriesResponse;
using db::LogEntry;
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

#define MIN_ELECTION_TIMEOUT   150
#define MAX_ELECTION_TIMEOUT   300
#define HEARTBEAT_TIMEOUT      70
#define SERVER_CNT             5

// Move to file with server configs and read from there upon start



// ***************************** State enum **********************************
enum State {FOLLOWER, CANDIDATE, LEADER};

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
int leaderID = 0;

State currState;
int commitIndex = 0; // index starts from 1
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

void executeLog() {
  while(true) {
    if(lastApplied < commitIndex) {
      lastApplied++;
      printf("Executing Log: %d\n", lastApplied);
      // Execute log at index lastApplied here 
      // Decrement lastApplied back to original value if there was a failure
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

void updateLog(std::vector<LogEntry> logEntries, std::vector<Log>::const_iterator logIndex, int leaderCommitIndex){
  Log logEntry;
  logs.erase(logIndex, logs.end());
  for(auto itr = logEntries.begin(); itr != logEntries.end(); itr++){
    logEntry.index = itr->index(); //change
    logEntry.term = currentTerm;
    logEntry.key = itr->key();
    logEntry.value = itr->value();
    logs.emplace(logIndex++, logEntry); 
    // persist in DB
    leveldb::Status logstatus = plogs->Put(leveldb::WriteOptions(), to_string(logEntry.index), logEntry.toString());
    // assert(status.ok()); should we add a try:catch here?
  }
  // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
  if(leaderCommitIndex > commitIndex)
    commitIndex = std::min(leaderCommitIndex, logs.empty() ? INT_MAX : logs.back().index);
}

void openOrCreateDBs() {

  leveldb::Options options;
  options.create_if_missing = true;

  leveldb::Status plogs_status = leveldb::DB::Open(options, "/tmp/plogs", &plogs);
  if (!plogs_status.ok()) std::cerr << plogs_status.ToString() << endl;
  assert(plogs_status.ok());

  leveldb::Status pmetadata_status = leveldb::DB::Open(options, "/tmp/pmetadata", &pmetadata);
  if(!pmetadata_status.ok()) std::cerr << pmetadata_status.ToString() << endl;
  assert(pmetadata_status.ok());

  leveldb::Status replicateddb_status = leveldb::DB::Open(options, "/tmp/replicateddb", &replicateddb);
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

// ***************************** RaftClient Code *****************************

class RaftClient {
  public:
    RaftClient(std::shared_ptr<Channel> channel);
    int PingOtherServers();
    int AppendEntries(int, int);

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

int RaftClient::AppendEntries(int logIndex, int lastIndex) {
  printf("[RaftClient::AppendEntries]\n");
  AppendEntriesRequest request;
  AppendEntriesResponse response;
  Status status;
  ClientContext context;

  int prevLogIndex = logIndex-1;
  // from this index + 1
  auto prevLogIndexIt = logs.begin();
  // to this index
  auto lastLogIndexIt = logs.end();
  for(; prevLogIndexIt != logs.end(); prevLogIndexIt++){
    if(prevLogIndexIt->index == prevLogIndex){
      break;
    }
  }
  for(; lastLogIndexIt != prevLogIndexIt; lastLogIndexIt--){
    if(lastLogIndexIt->index == lastIndex){
      break;
    }
  }

  request.set_term(currentTerm);
  request.set_leaderid(leaderID);
  request.set_prevlogindex(prevLogIndex);
  request.set_prevlogterm(prevLogIndexIt->term);
  request.set_leadercommitindex(commitIndex);

  // creating log entries to store
  for (auto nextIdxIt = prevLogIndexIt+1; nextIdxIt != lastLogIndexIt; nextIdxIt++) {
    db::LogEntry *reqEntry = request.add_entries();
    reqEntry->set_index(nextIdxIt->index);
    reqEntry->set_term(nextIdxIt->term);
    reqEntry->set_key(nextIdxIt->key);
    reqEntry->set_value(nextIdxIt->value);      
  }

  response.Clear();
  status = stub_->AppendEntries(&context, request, &response);

  if (status.ok()) {
    printf("[RaftClient::AppendEntries] RPC Success\n");
    if(response.success() == false){
      if(response.currterm() > currentTerm){
        printf("[RaftClient::AppendEntries] Higher Term in Response\n");
        return -3; // leader should convert to follower
      } else {
        printf("[RaftClient::AppendEntries] Term mismatch at prevLogIndex. Try with a lower nextIndex.\n");
        return -2; // decriment nextIndex
      }
    }
  } else {
    printf("[RaftClient::AppendEntries] RPC Failure\n");
    return -1;
  }
  return 0;
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

  Status AppendEntries(ServerContext *context, const AppendEntriesRequest *request, AppendEntriesResponse *response) override
  {
    printf("AppendEntries RPC received \n");
    //Process Append Entries RPC
    bool rpcSuccess = false;
    if(request->term() >= currentTerm){
      currentTerm = (int)request->term(); // updating current term
      electionTimer.reset(getRandomTimeout()); //election timer reset
      currState = FOLLOWER; // candidates become followers
      electionRunning = false; 
      
      int vectorIndex = -1;
      auto logAtPrevIndex = logs.begin();
      for(; logAtPrevIndex != logs.end(); logAtPrevIndex++){
        if(logAtPrevIndex->index == request->prevlogindex()){
          vectorIndex = logAtPrevIndex - logs.begin();
          break;
        }
      }

      // check if req->prevLogIndex exists in LevelDB
      bool existsInDB = false;
      string prevLogFromDB = "";
      if(vectorIndex == -1){
        leveldb::Status logstatus = 
                  plogs->Get(leveldb::ReadOptions(), to_string(request->prevlogindex()), &prevLogFromDB);
        if(logstatus.ok())
          existsInDB = true;
      }

      if((vectorIndex > -1) && (logs[vectorIndex].term == request->prevlogterm()) ||
        (existsInDB) && (Log(prevLogFromDB).term == request->prevlogterm()))  {
          //append and change commit index
          std::vector<db::LogEntry> logEntries(request->entries().begin(), request->entries().end());
          updateLog(logEntries, logAtPrevIndex+1, request->leadercommitindex());
          // updateLog should handle db update
          rpcSuccess = true;
      }
    } 
    response->set_currterm(currentTerm);
    response->set_success(rpcSuccess);
    return Status::OK;
  }

  Status Get(ServerContext *context, const GetRequest *req, GetResponse *resp) override
  {
    if(currState!= LEADER){
      resp->set_value("");
      resp->set_leaderid(leaderID);
      return Status(StatusCode::PERMISSION_DENIED, "Server not allowed top perform leader operations");
    }
    printf("Get invoked on server\n");
    string value;
    string key = req->key();
    leveldb::Status status = replicateddb->Get(leveldb::ReadOptions(), key, &value);
    printf("Value: %s\n", value.c_str());
    resp->set_value(value);
    return Status::OK;
  }
};

// Run main()

void RunGrpcServer(string server_address) {
  dbImpl service;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
}



int main(int argc, char **argv) {
  if(argc != 3) {
    printf("Usage: ./db_server <serverID> <server address>\n");
    return 0;
  }

  // initialize values 
  serverID = atoi(argv[1]);
  if(serverID == leaderID){
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

  // test log class - remove later
  Log l("1;1;name;Snehal");
  cout << l.index << " " << l.term << " " << l.key << " " << l.value << endl;
  cout << l.toString() << endl;  

  // initialize channels to servers

  // Run threads
  thread raftGrpcServer(RunGrpcServer, argv[2]);
  thread logExecutor(executeLog);
  thread raftServer(runRaftServer);

  raftGrpcServer.join();
  logExecutor.join();
  raftServer.join();
  return 0;
}
