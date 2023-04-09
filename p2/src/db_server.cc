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
using db::RvRequest;
using db::RvResponse;

#define MIN_ELECTION_TIMEOUT   3000
#define MAX_ELECTION_TIMEOUT   6000
#define HEARTBEAT_TIMEOUT      900
#define HEART   "\xE2\x99\xA5"
#define SPADE   "\xE2\x99\xA0"

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
    int RequestVote(int lastLogTerm, int lastLogIndex, int followerID);


  private:
    std::unique_ptr<RaftServer::Stub> stub_;
};

// ***************************** Volatile Variables **************************
int serverID;
int leaderID = -1; // put lock
int votesReceived = 0; // for a candidate

State currState; //put lock
int commitIndex = 0; // valid index starts from 1
int lastLogIndex = 0; // valid index starts from 1
int nextIndex[SERVER_CNT];
int matchIndex[SERVER_CNT];
int heartbeatSender[SERVER_CNT];
Timer electionTimer(1, MAX_ELECTION_TIMEOUT);
Timer heartbeatTimer(1, HEARTBEAT_TIMEOUT);
bool electionRunning = false; //put lock
bool appendEntriesRunning[SERVER_CNT];
RaftClient* clients[SERVER_CNT];

std::shared_mutex mutex_; // for heartbeatSender
std::shared_mutex mutex_ci; // for commitIndex
std::shared_mutex mutex_lli; // for lastLogIndex
std::shared_mutex mutex_votes; // for votesReceived
std::shared_mutex mutex_leader; // for leaderID
std::shared_mutex mutex_cs; // for currState
std::shared_mutex mutex_er; // for electionRunning
std::shared_mutex mutex_aer; // for appendEntriesRunning

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
std::shared_mutex mutex_ct; // for currentTerm
std::shared_mutex mutex_la; // for lastApplied
std::shared_mutex mutex_vf; // for votedFor

void setCurrState(State cs)
{
  mutex_cs.lock();
  currState = cs;
  mutex_cs.unlock();
  if(cs == LEADER) {
    mutex_leader.lock();
    leaderID = serverID;
    mutex_leader.unlock();
    printf("%s %s %s %s %s %s %s %s %s %s %s %s \n", SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE,SPADE);
  }
  printf("Server %d = %s for term = %d\n", serverID, stateNames[cs].c_str(), currentTerm);
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

void executeLog() {
  while(true) {
    mutex_ci.lock();
    int commitIndexLocal = commitIndex;
    mutex_ci.unlock();
    
    mutex_la.lock();
    if(lastApplied < commitIndexLocal) {
      printf("[ExecuteLog]: Executing log from index: %d\n", lastApplied+1); 
      
      // find lastApplied index in logs
      auto i = logs.begin();
      for(; i!=logs.end(); i++){
        if(i->index == lastApplied + 1){
          break;
        }
      }

      // This can happen when we have erased till min(matchIndex) which have not yet applied
      if(i== logs.end()){
        printf("[ExecuteLog]: Unable to find %d index in logs\n", lastApplied+1);
        continue;
        // TODO: if such case arises. Find in DB as well.
      }

      while(i!=logs.end() && i->index <= commitIndexLocal) {
        // put to replicateddb
        leveldb::Status status = replicateddb->Put(leveldb::WriteOptions(), i->key, i->value);
        if(!status.ok()){
          printf("[ExecuteLog]: Failure while put in replicated db %s\n", status.ToString().c_str());
          break;
        }
        lastApplied++;
        pmetadata->Put(leveldb::WriteOptions(), "lastApplied", to_string(lastApplied));
        i++;
      }
    }
    mutex_la.unlock();
  }
}

void invokeRequestVote(int followerID){
  // RequestVote, gather votes
  // should implement retries of RequestVote on unsuccessful returns
  int lastLogTerm = 0;
  if(logs.size()>0){
    lastLogIndex = logs.back().index;
    lastLogTerm = logs.back().term;
  }
  int ret = clients[followerID]->RequestVote(lastLogTerm, lastLogIndex,followerID);
  if (ret == 1) {
    mutex_votes.lock();
    votesReceived++;
    mutex_votes.unlock();
  }
  return;
}

void runElection() {
  mutex_votes.lock();
  votesReceived = 0;
  mutex_votes.unlock();

  mutex_ct.lock();
  currentTerm++;
  pmetadata->Put(leveldb::WriteOptions(), "currentTerm", to_string(currentTerm));
  mutex_ct.unlock();

  mutex_vf.lock();
  votedFor = serverID;
  pmetadata->Put(leveldb::WriteOptions(), "votedFor", to_string(votedFor));
  mutex_vf.unlock();

  mutex_votes.lock();
  votesReceived++;
  mutex_votes.unlock();

  printf("[runElection] Running Election for term=%d\n", currentTerm);
  // RequestVotes, gather votes

  thread RequestVoteThreads[SERVER_CNT];

  for(int id = 0; id<SERVER_CNT; id++) {
    if(id != serverID)
      RequestVoteThreads[id] = thread { invokeRequestVote, id };
  }

  // wait until all RequestVote threads have completed
  for(int id = 0; id<SERVER_CNT; id++) {
    if(id != serverID && RequestVoteThreads[id].joinable()){
      RequestVoteThreads[id].join();
    }

  }

  // no other server can become leader in this time for this term, 
  // because majority servers have already voted for this candidate.
  int majority = (SERVER_CNT+1)/2;
  printf("votesReceived = %d, Majority = %d\n", votesReceived, majority);
  if(votesReceived >= majority){
    printf("Candidate %d received majority of votes from available servers\n", serverID);
    setCurrState(LEADER);
  }else{
    setCurrState(FOLLOWER);
  }

  mutex_er.lock();
  electionRunning = false;
  mutex_er.unlock();

  return;
}

void runElectionTimer() {
  mutex_vf.lock();
  votedFor = -1;
  pmetadata->Put(leveldb::WriteOptions(), "votedFor", to_string(votedFor));
  mutex_vf.unlock();
  
  electionTimer.start(getRandomTimeout());
  while(electionTimer.running() && 
    electionTimer.get_tick() < electionTimer._timeout) ; // spin
  printf("[runElectionTimer] Spun for %d ms before timing out in state %d for term %d\n", electionTimer.get_tick(), currState, currentTerm);
  
  if(electionTimer.running()) {    
    mutex_er.lock();
    electionRunning = true;
    mutex_er.unlock();
    setCurrState(CANDIDATE);
    thread runElectionThread(runElection);
    runElectionThread.join();
    if(electionTimer.running()) runElectionTimer();
  }
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
  if(heartbeatTimer.running()) {
    sendHearbeat();
    runHeartbeatTimer();
  }
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
      mutex_ct.lock();
      // printf("[invokeAppendEntries] Sending Hearbeat to follower %d for term = %d\n", followerid, currentTerm);
      mutex_ct.unlock();
      status = sendAppendEntriesRpc(followerid, 0);
      heartbeatSender[followerid] = 0;
    }
    mutex_.unlock();
    mutex_lli.lock();
    if(followerid != serverID && nextIndex[followerid] <= lastLogIndex) {
      int lastidx = lastLogIndex;
      status = sendAppendEntriesRpc(followerid, lastidx);
    }
    mutex_lli.unlock();
    if(status == -1) break;
    mutex_aer.lock();
    if(!appendEntriesRunning[followerid]) {
      mutex_aer.unlock();
      break;
    } else mutex_aer.unlock();
  }
}

void runRaftServer() {
  thread electionTimerThread;
  thread heartbeatTimerThread;
  thread appendEntriesThreads[SERVER_CNT];
  while(true) {
    mutex_cs.lock();
    State currStateLocal = currState;
    mutex_cs.unlock();
    mutex_ct.lock();
    int ctLocal = currentTerm;
    mutex_ct.unlock();
    if(currStateLocal == FOLLOWER) {
      for(int i = 0; i<SERVER_CNT; i++) {
        mutex_aer.lock();
        if(appendEntriesRunning[i]) {
          appendEntriesRunning[i] = false;
          mutex_aer.unlock();
          appendEntriesThreads[i].join();
        } else
          mutex_aer.unlock();
      }
      if(heartbeatTimer.running()) {
        printf("[runRaftServer] In FOLLOWER with term = %d, stopping heartbeat timer.\n", ctLocal);
        heartbeatTimer.set_running(false);
        if(heartbeatTimerThread.joinable()) heartbeatTimerThread.join();
      }
      if(!electionTimer.running()) {
        printf("[runRaftServer] In FOLLOWER with term = %d, starting election timer.\n", ctLocal);
        // if(electionTimerThread.joinable()) electionTimerThread.join();
        electionTimer.set_running(true);
        electionTimerThread = thread { runElectionTimer };
      }
    }
    if(currStateLocal == CANDIDATE) {
      for(int i = 0; i<SERVER_CNT; i++) {
        mutex_aer.lock();
        if(appendEntriesRunning[i]) {
          appendEntriesRunning[i] = false;
          mutex_aer.unlock();
          appendEntriesThreads[i].join();
        } else 
          mutex_aer.unlock();
      }
      if(heartbeatTimer.running()) {
        printf("[runRaftServer] In CANDIDATE with term = %d, stopping heartbeat timer.\n", ctLocal);
        heartbeatTimer.set_running(false);
        heartbeatTimerThread.join();
      }
      mutex_er.lock();
      if(!electionRunning) {
        printf("[runRaftServer] In CANDIDATE with term = %d, starting election timer.\n", ctLocal);
        // if(electionTimerThread.joinable()) electionTimerThread.join();
        electionTimer.set_running(true);
        electionTimerThread = thread { runElectionTimer };
      }
      mutex_er.unlock();
    }
    if(currStateLocal == LEADER) {
      if(electionTimer.running()) {
        printf("[runRaftServer] In LEADER with term = %d, stopping election timer.\n", ctLocal);
        electionTimer.set_running(false);
        if(electionTimerThread.joinable()) electionTimerThread.join();
      }
      if(!heartbeatTimer.running()) {
        printf("[runRaftServer] In LEADER with term = %d, starting heartbeat timer.\n", ctLocal);
        heartbeatTimer.set_running(true);
        heartbeatTimerThread = thread { runHeartbeatTimer };
      }
      // init values and invoke append entries
      mutex_aer.lock();
      for(int i = 0; i<SERVER_CNT; i++) {
        if(!appendEntriesRunning[i]) {
          mutex_lli.lock();
          nextIndex[i] = lastLogIndex + 1;
          if(i == serverID) {
            matchIndex[i] = lastLogIndex;
            heartbeatSender[i] = -1;
          } else {
            matchIndex[i] = 0;
            heartbeatSender[i] = 0;
          }
          mutex_lli.unlock();
          appendEntriesRunning[i] = true;
          printf("[runRaftServer] Starting AppendEntriesInvoker for term = %d\n", ctLocal);
          appendEntriesThreads[i] = thread { invokeAppendEntries, i };
        }
      }
      mutex_aer.unlock();
      // check and update commit index 
      mutex_lli.lock();
      int lliLocal = lastLogIndex;
      mutex_lli.unlock();
      mutex_ci.lock();
      for(int N = lliLocal; N>commitIndex; N--) {
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
      mutex_ci.unlock();
    }
  }
}

void printRaftLog() {
  // printf("======================== Raft Log ===========================\n");
  // printf("Index Term  Key:Value\n");
  // for(auto logIt = logs.begin(); logIt != logs.end(); logIt++) {
  //   printf("%d  %d  %s:%s\n", logIt->index, logIt->term, (logIt->key).c_str(), (logIt->value).c_str());
  // }
  // printf("\nCommit Index: %d\n", commitIndex);
  // printf("Current Term: %d\n\n", currentTerm);
  // printf("=============================================================\n");
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
    logs.push_back(logEntry); 
    mutex_lli.lock();
    lastLogIndex = itr->index();
    mutex_lli.unlock();
    // persist in DB
    leveldb::Status logstatus = plogs->Put(leveldb::WriteOptions(), to_string(logEntry.index), logEntry.toString());
    // assert(status.ok()); should we add a try:catch here?
  }
  printRaftLog();
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
    mutex_ct.lock();
    currentTerm = stoi(value);
    mutex_ct.unlock();
    printf("[initializePersistedValues] currentTerm = %d\n", currentTerm);
  }

  value = "";
  leveldb::Status lastAppliedStatus = pmetadata->Get(leveldb::ReadOptions(), "lastApplied", &value);
  if(!lastAppliedStatus.ok()) {
    std::cerr << "[initializePersistedValues] lastApplied" << ": Error: " << lastAppliedStatus.ToString() << endl;
  } else {
    mutex_la.lock();
    lastApplied = stoi(value);
    mutex_la.unlock();
    printf("[initializePersistedValues] lastApplied = %d\n", lastApplied);
  }

  value = "";
  leveldb::Status votedForStatus = pmetadata->Get(leveldb::ReadOptions(), "votedFor", &value);
  if(!votedForStatus.ok()) {
    std::cerr << "[initializePersistedValues] votedFor" << ": Error: " << votedForStatus.ToString() << endl;
  } else {
    mutex_vf.lock();
    votedFor = stoi(value);
    mutex_vf.unlock();
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
  mutex_lli.lock();
  lastLogIndex = logidx - 1;
  mutex_lli.unlock();
  printf("[initializePersistedValues] Loaded logs till index = %d\n", logidx-1);
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
    int prevLogIndex = logIndex-1;
    // TODO: Uncomment if wiping in memory logs
    // // from this index + 1
    auto prevLogIndexIt = prevLogIndex == 0 ? --logs.begin() : logs.begin();
    for(auto iter = logs.begin(); iter != logs.end(); iter++){
      if(iter->index == prevLogIndex) {
        prevLogIndexIt = iter;
        break;
      }
    }
    request.set_term(currentTerm);
    request.set_leaderid(leaderID);
    request.set_prevlogindex(prevLogIndex);
    prevLogIndex == 0 ? request.set_prevlogterm(0) : request.set_prevlogterm(prevLogIndexIt->term);
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
    prevLogIndex == 0 ? request.set_prevlogterm(0) : request.set_prevlogterm(prevLogIndexIt->term);
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
        mutex_ct.lock();
        currentTerm = response.currterm();
        pmetadata->Put(leveldb::WriteOptions(), "currentTerm", to_string(currentTerm));
        pmetadata->Put(leveldb::WriteOptions(), "votedFor", to_string(-1));
        mutex_ct.unlock();
        return -3; // leader should convert to follower
      } else {
        printf("[RaftClient::AppendEntries] Term mismatch at prevLogIndex. Try with a lower nextIndex.\n");
        return -2; // decriment nextIndex
      }
    }
  } else {
    // if(lastIndex != 0)
    //   printf("[RaftClient::AppendEntries] RPC Failure\n");
    return -1;
  }
  return 0;
}

int RaftClient::RequestVote(int lastLogTerm, int lastLogIndex, int followerID){
  printf("[RequestVote]: RaftClient invoked\n");

  RvRequest request;
  RvResponse reply;
  Status status;
  ClientContext context;

  request.set_term(currentTerm);
  request.set_candidateid(serverID);
  request.set_lastlogterm(lastLogTerm);
  request.set_lastlogindex(lastLogIndex);

  reply.Clear();

  status = stub_->RequestVote(&context, request, &reply);

  if(status.ok()) {
    printf("[RequestVote]: RaftClient - RPC Success\n");
    if(reply.term() > currentTerm) {
      printf("[RequestVote]: RaftClient - Term of the server %d is higher than %d candidate\n", followerID, serverID);
    }
    if(reply.votegranted()){
      printf("[RequestVote]: RaftClient - Server %d granted vote for %d\n",followerID,serverID);
      return 1;
    }else{
      printf("[RequestVote]: RaftClient - Server %d did not vote for %d\n",followerID, serverID);
    }
  } else {

      if(status.error_code() == StatusCode::UNAVAILABLE){
        printf("[RequestVote]: RaftClient - Unavailable server\n");
      }
      printf("[RequestVote]: RaftClient - RPC Failure\n");
      return -1; // failure
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
    //Process Append Entries RPC
    bool rpcSuccess = false;
    if(request->term() >= currentTerm){
      mutex_leader.lock();
      leaderID = request->leaderid();
      // printf("Setting LeaderID = %d\n", leaderID);
      mutex_leader.unlock();

      mutex_ct.lock();
      currentTerm = (int)request->term(); // updating current term
      pmetadata->Put(leveldb::WriteOptions(), "currentTerm", to_string(currentTerm));
      mutex_ct.unlock();

      electionTimer.reset(getRandomTimeout()); //election timer reset
      
      mutex_cs.lock();
      State csLocal = currState;
      mutex_cs.unlock();
      if(csLocal != FOLLOWER) // candidates become followers
        setCurrState(FOLLOWER); 

      int leaderCommitIndex = request->leadercommitindex();
      // if(request->entries().size() == 0)
        // printf("%s %s %s Heartbeat from Leader: %d for Term: %d %s %s %s\n", HEART, HEART, HEART, leaderID, currentTerm, HEART, HEART, HEART);
        // printf("[AppendEntries RPC] Received for, term = %d, prevLogIndex=%d, prevLogTerm=%d, No of entries=%d\n",
        // request->term(), request->prevlogindex(), request->prevlogterm(), request->entries().size());
      // Look for prevLogIndex in local logs
      int vectorIndex = -1;
      auto logAtPrevIndex = --logs.begin();
      for(auto iter = logs.begin(); iter != logs.end(); iter++){
        if(iter->index == request->prevlogindex()){
          logAtPrevIndex = iter;
          vectorIndex = logAtPrevIndex - logs.begin();
          break;
        }
      }
      // printf("[AppendEntries RPC] vectorIndex = %d\n", vectorIndex);
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
          if(request->entries().size() > 0) {
            std::vector<db::LogEntry> logEntries(request->entries().begin(), request->entries().end());
            updateLog(logEntries, ++logAtPrevIndex, request->leadercommitindex());
          }
          // updateLog should handle db update
          rpcSuccess = true;
      }
      // }
      // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
      mutex_ci.lock();
      mutex_lli.lock();
      if(leaderCommitIndex > commitIndex)
        commitIndex = std::min(leaderCommitIndex, lastLogIndex);
      mutex_lli.unlock();
      mutex_ci.unlock();
    } 
    response->set_currterm(currentTerm);
    response->set_success(rpcSuccess);
    return Status::OK;
  }

  Status RequestVote(ServerContext *context, const RvRequest *req, RvResponse *resp) override
  {
    int term = req->term();
    int candidateID = req->candidateid();
    int lli = req->lastlogindex();
    int llt = req->lastlogterm();

    mutex_cs.lock();
    printf("[RequestVote] invoked on %s %d by candidate %d for term %d\n", stateNames[currState].c_str(), serverID, candidateID, term);
    mutex_cs.unlock();

    mutex_ct.lock();
    int ctLocal = currentTerm;
    mutex_ct.unlock();


    if (term < ctLocal){ // curr server has a greater term than candidate so it will not vote
      resp->set_term(ctLocal);
      resp->set_votegranted(false);
      printf("NOT voting: term %d < currentTerm %d\n", term, ctLocal);

      return Status::OK;
    }
    if(term > ctLocal){
      // JUST UPDATE CURRENTTERM AND DON"T VOTE
      // TODO: Discuss reasons
      /* just update currentTerm and don't vote.
      Reason 1: if the current leader which is alive and has same currentTerm can receive 
      this candidate's term on next appendEntries response becomes a follower.
      Reason 2: incase of no leader in this candidate's term, this vote should 
      */
      mutex_ct.lock();
      currentTerm = term;
      ctLocal = term;
      pmetadata->Put(leveldb::WriteOptions(), "currentTerm", to_string(currentTerm));
      mutex_ct.unlock();

      mutex_vf.lock();
      votedFor = -1;
      pmetadata->Put(leveldb::WriteOptions(), "votedFor", to_string(votedFor));
      mutex_vf.unlock();
      // IMP: whenever currentTerm is increased we should also update votedFor to -1, should check AppendEntries also for such scenarios.

      setCurrState(FOLLOWER);
      electionTimer.reset(getRandomTimeout());
      resp->set_term(ctLocal); 
      resp->set_votegranted(false);
      printf("[RequestVote]Candidate %d has higher term than me, updating current term.\n", candidateID);
    }
    
    // else if(term == ctLocal){ // that means someBody has already sent the requestVote as it has already seen this term     
    mutex_vf.lock();
    if(votedFor == -1 || votedFor == candidateID) {
      int voter_lli = 0;
      int voter_llt = 0;
      if(logs.size()>0){
        voter_lli = logs.back().index;
        voter_llt = logs.back().term;
      }

      if(llt > voter_llt || (llt == voter_llt && lli >= voter_lli)) { // candidate has longer log than voter or ..
        resp->set_term(ctLocal); 
        resp->set_votegranted(true);
        electionTimer.reset(getRandomTimeout());
        printf("llt = %d \nvoter_llt = %d \nlli = %d \nvoter_lli = %d\n", llt, voter_llt, lli, voter_lli);
        printf("VOTED!: Candidate has longer log than me\n");
        mutex_ct.lock();
        currentTerm = term;
        pmetadata->Put(leveldb::WriteOptions(), "currentTerm", to_string(currentTerm));
        mutex_ct.unlock();
        votedFor = candidateID;
        pmetadata->Put(leveldb::WriteOptions(), "votedFor", to_string(candidateID));
      } else {
        resp->set_term(ctLocal); 
        resp->set_votegranted(false);
        printf("llt = %d \nvoter_llt = %d \nlli = %d \nvoter_lli = %d\n");
        printf("NOT voting: I have most recent log or longer log\n");
      }
      mutex_vf.unlock();
      return Status::OK;
    } else {
      resp->set_term(ctLocal);
      resp->set_votegranted(false);
      printf("NOT voting: votedFor %d\n", votedFor);
      mutex_vf.unlock();
      return Status::OK;
    }
    mutex_vf.unlock();
    // }

    // anything that doesn't follow the above condition don't vote!
    return Status::OK;
  }

  Status Get(ServerContext *context, const GetRequest *req, GetResponse *resp) override
  {
    mutex_cs.lock();
    printf("Get request invoked on %s\n", stateNames[currState].c_str());
    if(currState!= LEADER){
      resp->set_value("");
      mutex_leader.lock();
      printf("Setting leaderID to %d\n", leaderID);
      resp->set_leaderid(leaderID);
      mutex_leader.unlock();
      resp->set_db_errno(EPERM);
      mutex_cs.unlock();
      return Status::OK;
    }
    mutex_cs.unlock();
    string value;
    string key = req->key();
    leveldb::ReadOptions options;
    options.fill_cache = false;
    leveldb::Status status = replicateddb->Get(options, key, &value);
    printf("Value: %s\n", value.c_str());
    resp->set_value(value);
    mutex_leader.lock();
    resp->set_leaderid(leaderID);
    mutex_leader.unlock();
    return Status::OK;
  }
  
  Status Put(ServerContext *context, const PutRequest *req, PutResponse *resp) override
  {
    mutex_cs.lock();
    printf("Put request invoked on %s\n", stateNames[currState].c_str());
    if(currState!= LEADER){
      mutex_leader.lock();
      printf("Setting leaderID to %d\n", leaderID);
      resp->set_leaderid(leaderID);
      mutex_leader.unlock();
      resp->set_db_errno(EPERM);
      mutex_cs.unlock();
      return Status::OK;
    }
    mutex_cs.unlock();
    string value = req->value();
    string key = req->key();

    Log logEntry;

    mutex_lli.lock();
    int lli = lastLogIndex;
    mutex_lli.unlock();
    
    logEntry.index = ++lli;
    
    mutex_ct.lock();
    logEntry.term = currentTerm;
    mutex_ct.unlock();

    logEntry.key = key;
    logEntry.value = value;
    logs.push_back(logEntry);

    mutex_lli.lock();
    lastLogIndex = logEntry.index;
    mutex_lli.unlock();


    leveldb::Status logstatus = plogs->Put(leveldb::WriteOptions(), to_string(logEntry.index), logEntry.toString());

    printRaftLog();

    while(true) {
      mutex_ci.lock();
      if(commitIndex >= lli) {
        mutex_ci.unlock();
        break;
      } 
      mutex_ci.unlock();
    }
    printf("[Put] Success: (%s, %s) log committed\n", key.c_str(), value.c_str());

    printRaftLog();

    mutex_leader.lock();
    resp->set_leaderid(leaderID);
     mutex_leader.unlock();
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
