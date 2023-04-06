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
using db::PutRequest;
using db::PutResponse;

#define MIN_ELECTION_TIMEOUT   4000
#define MAX_ELECTION_TIMEOUT   5000
#define HEARTBEAT_TIMEOUT      1000

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

// ************************ RaftClient class definition *********************

class RaftClient {
  public:
    RaftClient(std::shared_ptr<Channel> channel);
    int PingOtherServers();
    int AppendEntries(int, int);

  private:
    std::unique_ptr<RaftServer::Stub> stub_;
};

// ***************************** Volatile Variables **************************
int serverID;
int leaderID = -1;

State currState;
int commitIndex = 0; // valid index starts from 1
int lastLogIndex = 0; // valid index starts from 1
int nextIndex[SERVER_CNT];
int matchIndex[SERVER_CNT];
int heartbeatSender[SERVER_CNT];
Timer electionTimer(1, MAX_ELECTION_TIMEOUT);
Timer heartbeatTimer(1, HEARTBEAT_TIMEOUT);
bool electionRunning = false;
bool appendEntriesRunning[SERVER_CNT];
RaftClient* clients[SERVER_CNT];
std::shared_mutex mutex_;

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

bool greaterThanMajority(int arr[], int N) {
  int majcnt = (SERVER_CNT+1)/2;
  for(int i = 0; i<SERVER_CNT; i++) {
    if(arr[i] >= N) majcnt--;
  }
  if(majcnt > 0) return false;
  return true;
}

// hardcoded for testing executeLog: REMOVE LATER
// void testExecuteLog(){
//   printf("in testExecuteLog\n");
//   Log logEntryOne = Log(1, currentTerm, "key1", "value1");
//   Log logEntryTwo = Log(1, currentTerm, "key2", "value2");
//   Log logEntryThree = Log(1, currentTerm, "key3", "value3");
//   logs.push_back(logEntryOne);
//   logs.push_back(logEntryTwo);
//   logs.push_back(logEntryThree); 
//   commitIndex = 4;
//   lastApplied = 0;
// }

void executeLog() {
  // testExecuteLog();
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
  printf("[runElectionTimer] Getting random timeout %d\n", timeout);
  electionTimer.start(timeout);
  while(electionTimer.running() && 
    electionTimer.get_tick() < electionTimer._timeout) ; // spin
  printf("[runElectionTimer] Spun for %d ms before timing out in state %d for term %d\n", electionTimer.get_tick(), currState, currentTerm);
  runElection();
  setCurrState(CANDIDATE);
}

void sendHearbeat(){
  for(int i = 0; i<SERVER_CNT; i++) if(i != serverID) {
    mutex_.lock();
    heartbeatSender[i] = 1;
    mutex_.unlock();
  }
}

void runHeartbeatTimer() {
  heartbeatTimer.start(HEARTBEAT_TIMEOUT);
  while(heartbeatTimer.running() &&
    heartbeatTimer.get_tick() < heartbeatTimer._timeout) ; // spin
  sendHearbeat();
  runHeartbeatTimer();
}

int sendAppendEntriesRpc(int followerid, int lastidx){
  int ret = clients[followerid]->AppendEntries(nextIndex[followerid], lastidx);
  if(ret == 0) { // success
    if(lastidx != 0) {
      printf("[sendAppendEntriesRpc] AppendEntries successful for followerid = %d, startidx = %d, endidx = %d\n", followerid, nextIndex[followerid], lastidx);
      nextIndex[followerid] = lastidx + 1;
      matchIndex[followerid] = lastidx;
    }
  }
  if(ret == -2) { // log inconsistency
    printf("[sendAppendEntriesRpc] AppendEntries failure; Log inconsistency for followerid = %d, new nextIndex = %d\n", followerid, nextIndex[followerid]);
    nextIndex[followerid]--;
  }
  if(ret == -3) { // term of follower bigger, convert to follower
    printf("[sendAppendEntriesRpc] AppendEntries failure; Follower (%d) has bigger term (new term = %d), converting to follower.\n", followerid, currentTerm);
    pmetadata->Put(leveldb::WriteOptions(), "currentTerm", to_string(currentTerm));
    setCurrState(FOLLOWER);
    return -1;
  }
  return 0;
}

void invokeAppendEntries(int followerid) {
  while(true) {
    int status = 0;
    if(followerid == serverID) matchIndex[followerid] = lastLogIndex;
    mutex_.lock();
    if(followerid != serverID && heartbeatSender[followerid] == 1){
      status = sendAppendEntriesRpc(followerid, 0);
      heartbeatSender[followerid] = 0;
    }
    mutex_.unlock();
    if(followerid != serverID && nextIndex[followerid] <= lastLogIndex) {
      int lastidx = lastLogIndex;
      status = sendAppendEntriesRpc(followerid, lastidx);
    }
    if(status == -1) break;
  }
}

void runRaftServer() {
  thread electionTimerThread;
  thread heartbeatTimerThread;
  thread appendEntriesThreads[SERVER_CNT];
  while(true) {
    if(currState == FOLLOWER) {
      for(int i = 0; i<SERVER_CNT; i++) {
        if(appendEntriesRunning[i]) {
          appendEntriesRunning[i] = false;
          appendEntriesThreads[i].join();
        }
      }
      if(heartbeatTimer.running()) {
        printf("[runRaftServer] In FOLLOWER, stopping heartbeat timer.\n");
        heartbeatTimer.set_running(false);
        heartbeatTimerThread.join();
      }
      if(!electionTimer.running()) {
        printf("[runRaftServer] In FOLLOWER, starting election timer.\n");
        if(electionTimerThread.joinable()) electionTimerThread.join();
        electionTimer.set_running(true);
        electionTimerThread = thread { runElectionTimer };
      }
    }
    if(currState == CANDIDATE) {
      for(int i = 0; i<SERVER_CNT; i++) {
        if(appendEntriesRunning[i]) {
          appendEntriesRunning[i] = false;
          appendEntriesThreads[i].join();
        }
      }
      if(heartbeatTimer.running()) {
        printf("[runRaftServer] In CANDIDATE, stopping heartbeat timer.\n");
        heartbeatTimer.set_running(false);
        heartbeatTimerThread.join();
      }
      if(!electionRunning) {
        printf("[runRaftServer] In CANDIDATE, starting election timer.\n");
        if(electionTimerThread.joinable()) electionTimerThread.join();
        electionTimer.set_running(true);
        electionTimerThread = thread { runElectionTimer };
      }
    }
    if(currState == LEADER) {
      if(electionTimer.running()) {
        printf("[runRaftServer] In LEADER, stopping election timer.\n");
        electionTimer.set_running(false);
        electionTimerThread.join();
      }
      if(!heartbeatTimer.running()) {
        printf("[runRaftServer] In LEADER, starting heartbeat timer.\n");
        heartbeatTimer.set_running(true);
        heartbeatTimerThread = thread { runHeartbeatTimer };
      }
      // init values and invoke append entries
      for(int i = 0; i<SERVER_CNT; i++) {
        if(!appendEntriesRunning[i]) {
          nextIndex[i] = lastLogIndex + 1;
          if(i == serverID) {
            matchIndex[i] = lastLogIndex;
            heartbeatSender[i] = -1;
          } else {
            matchIndex[i] = 0;
            heartbeatSender[i] = 0;
          }
          appendEntriesRunning[i] = true;
          appendEntriesThreads[i] = thread { invokeAppendEntries, i };
        }
      }
      // check and update commit index 
      for(int N = lastLogIndex; N>commitIndex; N--) {
        auto NLogIndexIt = logs.end();
        for(; NLogIndexIt != logs.begin(); NLogIndexIt--) {
          if(NLogIndexIt->index == N) break;
        }
        if(greaterThanMajority(matchIndex, N) && NLogIndexIt->term == currentTerm) {
          printf("[runRaftServer] LEADER: Commiting index = %d\n", N);
          commitIndex = N;
          break;
        }
      }
    }
  }
}

void printRaftLog() {
  printf("======================== Raft Log ===========================\n");
  for(auto logIt = logs.begin(); logIt != logs.end(); logIt++) {
    printf("%d  %d  %s:%s\n", logIt->index, logIt->term, (logIt->key).c_str(), (logIt->value).c_str());
  }
  printf("\nCommit Index: %d\n\n", commitIndex);
  printf("=============================================================\n");
}

void updateLog(std::vector<LogEntry> logEntries, std::vector<Log>::const_iterator logIndex, int leaderCommitIndex){
  Log logEntry;
  logs.erase(logIndex, logs.end());
  // delete from DB
  for(auto itr = logEntries.begin(); itr != logEntries.end(); itr++){
    logEntry.index = itr->index(); //change
    logEntry.term = itr->term();
    logEntry.key = itr->key();
    logEntry.value = itr->value();
    logs.emplace(logIndex++, logEntry); 
    lastLogIndex = itr->index();
    // persist in DB
    leveldb::Status logstatus = plogs->Put(leveldb::WriteOptions(), to_string(logEntry.index), logEntry.toString());
    // assert(status.ok()); should we add a try:catch here?
  }
  printRaftLog();
  // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
  if(leaderCommitIndex > commitIndex)
    commitIndex = std::min(leaderCommitIndex, lastLogIndex);
}

void openOrCreateDBs() {
  leveldb::Options options;
  options.create_if_missing = true;

  leveldb::Status plogs_status = leveldb::DB::Open(options, "/tmp/plogs" + to_string(serverID), &plogs);
  if (!plogs_status.ok()) std::cerr << plogs_status.ToString() << endl;
  assert(plogs_status.ok());
  printf("[openOrCreateDBs] Successfully opened plogs DB.\n");

  leveldb::Status pmetadata_status = leveldb::DB::Open(options, "/tmp/pmetadata" + to_string(serverID), &pmetadata);
  if(!pmetadata_status.ok()) std::cerr << pmetadata_status.ToString() << endl;
  assert(pmetadata_status.ok());
  printf("[openOrCreateDBs] Successfully opened pmetadata DB.\n");

  leveldb::Status replicateddb_status = leveldb::DB::Open(options, "/tmp/replicateddb" + to_string(serverID), &replicateddb);
  if(!replicateddb_status.ok()) std::cerr << replicateddb_status.ToString() << endl;
  assert(replicateddb_status.ok());
  printf("[openOrCreateDBs] Successfully opened replicateddb DB.\n");
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
  lastLogIndex = logidx - 1;
  printf("[initializePersistedValues] Loaded logs till index = %d\n", lastLogIndex);
  printRaftLog();
}

// ***************************** RaftClient Code *****************************

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
  // printf("[RaftClient::AppendEntries]Entering\n");
  AppendEntriesRequest request;
  AppendEntriesResponse response;
  Status status;
  ClientContext context;

  if(lastIndex == 0){
    request.Clear();
    request.set_term(currentTerm);
    request.set_leaderid(leaderID);
    request.set_leadercommitindex(commitIndex);
  } else {
    int prevLogIndex = logIndex-1;
    // from this index + 1
    auto prevLogIndexIt = prevLogIndex == 0 ? --logs.begin() : logs.begin();
    // to this index
    auto lastLogIndexIt = logs.end();
    for(auto iter = logs.begin(); iter != logs.end(); iter++){
      if(iter->index == prevLogIndex) {
        prevLogIndexIt = iter;
        break;
      }
    }

    for(auto iter = logs.end(); iter != prevLogIndexIt; iter--){
      if(iter->index == lastIndex){
        lastLogIndexIt = ++iter;
        break;
      }
    }

    request.set_term(currentTerm);
    request.set_leaderid(leaderID);
    request.set_prevlogindex(prevLogIndex);
    request.set_prevlogterm(prevLogIndexIt->term);
    request.set_leadercommitindex(commitIndex);

    // creating log entries to store

    for (auto nextIdxIt = ++prevLogIndexIt; nextIdxIt != lastLogIndexIt; nextIdxIt++) {
      db::LogEntry *reqEntry = request.add_entries();
      reqEntry->set_index(nextIdxIt->index);
      reqEntry->set_term(nextIdxIt->term);
      reqEntry->set_key(nextIdxIt->key);
      reqEntry->set_value(nextIdxIt->value);      
    }
  }

  response.Clear();
  status = stub_->AppendEntries(&context, request, &response);

  if (status.ok()) {
    if(lastIndex != 0)
      printf("[RaftClient::AppendEntries] RPC Success\n");
    if(response.success() == false){
      if(response.currterm() > currentTerm){
        printf("[RaftClient::AppendEntries] Higher Term in Response\n");
        currentTerm = response.currterm();
        return -3; // leader should convert to follower
      } else {
        printf("[RaftClient::AppendEntries] Term mismatch at prevLogIndex. Try with a lower nextIndex.\n");
        return -2; // decriment nextIndex
      }
    }
  } else {
    if(lastIndex != 0)
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
    printf("[AppendEntries RPC] received \n");
    //Process Append Entries RPC
    bool rpcSuccess = false;
    if(request->term() >= currentTerm){
      currentTerm = (int)request->term(); // updating current term
      electionTimer.reset(getRandomTimeout()); //election timer reset
      currState = FOLLOWER; // candidates become followers
      electionRunning = false; 
      
      if(request->entries().size() == 0){
        rpcSuccess = true;
        // update commit index
        int leaderCommitIndex = request->leadercommitindex();
        if(leaderCommitIndex > commitIndex) commitIndex = std::min(leaderCommitIndex, lastLogIndex);
      } else {
        int vectorIndex = -1;
        auto logAtPrevIndex = --logs.begin();
        for(auto iter = logs.begin(); iter != logs.end(); iter++){
          if(iter->index == request->prevlogindex()){
            logAtPrevIndex = iter;
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

        if((request->prevlogindex() == 0) || 
          (vectorIndex > -1) && (logs[vectorIndex].term == request->prevlogterm()) ||
          (existsInDB) && (Log(prevLogFromDB).term == request->prevlogterm()))  {
            //append and change commit index
            std::vector<db::LogEntry> logEntries(request->entries().begin(), request->entries().end());
            updateLog(logEntries, ++logAtPrevIndex, request->leadercommitindex());
            // updateLog should handle db update
            rpcSuccess = true;
        }
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
    logEntry.index = lastLogIndex+1;
    logEntry.term = currentTerm;
    logEntry.key = key;
    logEntry.value = value;

    logs.push_back(logEntry);
    lli = logEntry.index;
    lastLogIndex = logEntry.index;
    mutex.unlock();

    printRaftLog();

    while(commitIndex< lli){
      // printf("Waiting for commitIndex: %d == lastLogIndex: %d\n", commitIndex, lli);
    }
    printf("[Put] Success: (%s, %s) log committed\n", key.c_str(), value.c_str());

    printRaftLog();

    resp->set_leaderid(leaderID);
    return Status::OK;
  }
};

void RunGrpcServer(string server_address) {
  dbImpl service;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server = builder.BuildAndStart();
  std::cout << "[RunGrpcServer] Server listening on " << server_address << std::endl;
  server->Wait();
}

// void testPut(){
//   currentTerm++;
//   Log logEntryOne = Log(0, currentTerm, "name", "Ritu"); // need something at index = 0?
//   logs.push_back(logEntryOne); // hardcoded for testing put: REMOVE LATER
// }

int main(int argc, char **argv) {
  if(argc != 3) {
    printf("Usage: ./db_server <serverID> <server address>\n");
    return 0;
  }

  // initialize values 
  serverID = atoi(argv[1]);
  // if(serverID == 0) {
  //   setCurrState(LEADER); 
  // }
  // else setCurrState(FOLLOWER);
  setCurrState(FOLLOWER);

  electionTimer.set_running(false);
  heartbeatTimer.set_running(false); 

  // open leveldb DBs
  openOrCreateDBs();
  // read from persisted variables
  initializePersistedValues();

  for(int i = 0; i<SERVER_CNT; i++) {
    nextIndex[i] = lastLogIndex + 1;
    if(i == serverID) matchIndex[i] = lastLogIndex;
    else matchIndex[i] = 0;
  }

  //test get operation related operations - REMOVE LATER
  replicateddb->Put(leveldb::WriteOptions(), "name", "Ritu");

  // testPut();

  // initialize channels to servers
  for(int i = 0; i<SERVER_CNT; i++) {
    if(i != serverID) {
      clients[i] = new RaftClient(grpc::CreateChannel(serverIPs[i], grpc::InsecureChannelCredentials()));
    }
  }

  // Run threads
  thread logExecutor(executeLog);
  thread raftServer(runRaftServer);
  RunGrpcServer(argv[2]);

  if(logExecutor.joinable()) logExecutor.join();
  if(raftServer.joinable()) raftServer.join();
  return 0;
}
