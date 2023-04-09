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
using std::stoi;
using std::to_string;

#define SERVER_ADDR           "0.0.0.0:50052"
typedef std::unique_ptr<RaftServer::Stub> ServerStub;

std::fstream leaderidFile;
uint32_t leaderID = 0;

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
    printf("Init connection %d\n", id);
    ServerStub stub;
    stub = RaftServer::NewStub(grpc::CreateChannel(serverIPs[id], grpc::InsecureChannelCredentials()));

	printf("initgRPC: Client is contacting server: %s\n", serverIPs[id].c_str());
    return stub;
}

int DbClient::Ping(int *round_trip_time)
{
    auto start = std::chrono::steady_clock::now();

    PingMessage request;
    PingMessage reply;
    Status status;

    ClientContext context;
    status = stub_->Ping(&context, request, &reply);

    if (status.ok()) {

        return 0;
    }
    else {
        return -1;
    }
}

/*
Test Cases:
1. leader = 0, (server 0 up) client calls it in first go and gets positive response 
2. leader = 1, (servers 0, 1 up) client calls 0 then 0 redirects to 1, in the next trial calls get on leader server 1. 
3. leader = 2, (servers 0, 2 up) client calls 0 then 0 redirects to 2, in the next trial calls get on leader server 2.
4. leader = 2, (servers 1, 2 up) clients calls 0 twice, max retries exhausted, calls 1, 1 redirects to leader 2, client then calls 2
5. leader = 0 (servers 1 up) client calls 0 twice, then calls 1, redirects to 0, try again once just in case leader = 0 has been elected and up during that time.
6. leader = 1 

*/

int DbClient::Get(string key, string value) {
    printf("[Get]: Invoked;\n");

    GetRequest req;
    GetResponse res;
    Status status;

    // ClientContext context;
    req.set_key(key);
    res.Clear();

    bool leaderFound = false;
    int maxRetries = 2;
    int retries[SERVER_CNT];
    for(int i=0; i<SERVER_CNT; i++){
        retries[i] = maxRetries;
    }

    string lid;
    leaderidFile.seekg(0);
    leaderidFile >> lid;
    if(!lid.empty()) {
        leaderID = stoi(lid);
        leaderFound = true;
        printf("Leader id found to be %d\n", leaderID);
    } else { 
        printf("Leaderid file was empty/new file\n");
    }
    

    for(int id = 0; id < SERVER_CNT && !leaderFound; id++){
        if(retries[id] == 0){
            continue; // try next server if the max retries for server with id are completed.
        }
        ServerStub stub = initConnection(id);
        ClientContext context;
        res.Clear();

        status = stub->Get(&context, req, &res);
        printf("Status: %d\n", status.ok());

        if (status.ok()) { // the request was called on leader
            int server_errno = res.db_errno();
            if (server_errno) {
                if(server_errno == EPERM) { // req called on follower
                    if(res.leaderid() >=0 && res.leaderid()<5){ // correct leaderID returned by follower
                        leaderFound = true;
                        leaderID = res.leaderid(); // set correct leaderID which was returned by follower
                        leaderidFile.seekg(0);
                        leaderidFile << to_string(leaderID);
                        printf("ServerID = %d, leaderID sent by follower: %d \n", id, res.leaderid());
                        break; // break to call get on the correct leaderID
                    }
                    continue; // leaderID was not correctly returned by follower so try get on the next server
                }
                printf("[Get]: Error %d on server.\n", server_errno);
                printf("[Get]: Function ended due to server failure.\n");
                errno = server_errno;
                return -1;
            } else {
                leaderidFile.seekg(0);
                leaderidFile << to_string(id);
                printf("New leader id = %d\n", id);
            }

            value = res.value();
            printf("Value: %s\n", value.c_str());
            printf("leaderID: %d\n", res.leaderid());
            
            printf("[Get]: Function ended;\n");
            return 0;
        } else { // either req did not successfully run on leader or req called on follower
            errno = RPCstatusCode(status.error_code());
            
            if(errno == ETIMEDOUT){ // server called was unavailable, retry
                printf("Server %d not available\n", id);
                retries[id]--;
                id--;
                continue;
            }
            // failure in executing RPC on leader or follower
            printf("[Get]: RPC failure\n");
            return -1;
        }
    }

    while(leaderFound) {
        ServerStub stub = initConnection(leaderID);
        ClientContext context;
        status = stub->Get(&context, req, &res);
        if (status.ok()) {
            int server_errno = res.db_errno();
            if(server_errno == EPERM) { // req called on follower
                if(res.leaderid() >=0 && res.leaderid()<5){ // correct leaderID returned by follower
                    leaderFound = true;
                    leaderID = res.leaderid(); // set correct leaderID which was returned by follower
                    leaderidFile.seekg(0);
                    leaderidFile << to_string(leaderID);
                    printf("Leader changed! New leaderid = %d \n", res.leaderid());
                }
                continue;
            } else if(server_errno) {
                printf("[Get]: Error %d on server.\n", server_errno);
                printf("[Get]: Function ended due to server failure.\n");
                errno = server_errno;
                return -1;
            }
            value = res.value();
            printf("Value: %s\n", value.c_str());
            printf("[Get]: Function ended;\n");
            return 0;
        } else {
            errno = RPCstatusCode(status.error_code());
            printf("[Get]: RPC failure\n");
            leaderID = (leaderID+1)%5;
            // return -1;
        }
    }
    if(!leaderFound) {
        printf("Leader not Found\n");
        return -1;
    }
}


int DbClient::Put(string key, string value) {
    printf("[Put]: Invoked;\n");

    PutRequest req;
    PutResponse res;
    Status status;

    // ClientContext context;
    req.set_key(key);
    req.set_value(value);
    res.Clear();

    bool leaderFound = false;
    int maxRetries = 2;
    int retries[SERVER_CNT];
    for(int i=0; i<SERVER_CNT; i++){
        retries[i] = maxRetries;
    }

    string lid;
    leaderidFile.seekg(0);
    leaderidFile >> lid;
    if(!lid.empty()) {
        leaderID = stoi(lid);
        leaderFound = true;
    }

    for(int id = 0; id < SERVER_CNT && !leaderFound; id++){
        if(retries[id] == 0){
            continue; // try next server if the max retries for server with id are completed.
        }
        ServerStub stub = initConnection(id);
        ClientContext context;
        res.Clear();

        status = stub->Put(&context, req, &res);
        printf("Status: %d\n", status.ok());
        
        if (status.ok()) { // the request was called on leader
            int server_errno = res.db_errno();
            if (server_errno) {
                if(server_errno == EPERM) { // req called on follower
                    if(res.leaderid() >=0 && res.leaderid()<5){ // correct leaderID returned by follower
                        leaderFound = true;
                        leaderID = res.leaderid(); // set correct leaderID which was returned by follower
                        leaderidFile.seekg(0);
                        leaderidFile << to_string(leaderID);
                        printf("ServerID = %d, leaderID sent by follower: %d \n", id, res.leaderid());
                        break; // break to call Put on the correct leaderID
                    }
                    continue; // leaderID was not correctly returned by follower so try Put on the next server
                }
                printf("[Put]: Error %d on server.\n", server_errno);
                printf("[Put]: Function ended due to server failure.\n");
                errno = server_errno;
                return -1;
            } else {
                leaderidFile.seekg(0);
                leaderidFile << to_string(id);
                printf("New leader id = %d\n", id);
            }

            // value = res.value();
            // printf("Value: %s\n", value.c_str());
            printf("leaderID: %d\n", res.leaderid());
            
            printf("[Put]: Function ended;\n");
            return 0;
        } else { // either req did not successfully run on leader or req called on follower
            errno = RPCstatusCode(status.error_code());
            
            if(errno == ETIMEDOUT){ // server called was unavailable, retry
                printf("Server %d not available\n", id);
                retries[id]--;
                id--;
                continue;
            }
            // failure in executing RPC on leader or follower
            printf("[Put]: RPC failure\n");
            return -1;
        }
    }

    while(leaderFound) {
        ServerStub stub = initConnection(leaderID);
        ClientContext context;
        status = stub->Put(&context, req, &res);
        
        if (status.ok()) {
            int server_errno = res.db_errno();
            if(server_errno == EPERM) { // req called on follower
                if(res.leaderid() >=0 && res.leaderid()<5){ // correct leaderID returned by follower
                    leaderFound = true;
                    leaderID = res.leaderid(); // set correct leaderID which was returned by follower
                    leaderidFile.seekg(0);
                    leaderidFile << to_string(leaderID);
                    printf("Leader changed! New leaderID = %d \n", res.leaderid());
                }
                continue;
            } else if(server_errno) {
                printf("[Put]: Error %d on server.\n", server_errno);
                printf("[Put]: Function ended due to server failure.\n");
                errno = server_errno;
                return -1;
            }
            printf("[Put]: Function ended;\n");
            return 0;
        } else {
            errno = RPCstatusCode(status.error_code());
            printf("[Get]: RPC failure\n");
            leaderID = (leaderID+1)%5;
            // return -1;
        }
    }
    if(!leaderFound) {
        printf("Leader not Found\n");
        return -1;
    }

}

int main(int argc, char **argv)
{
    string key, value;
    DbClient* client;
    client = new DbClient();

    leaderidFile.open("leaderid.txt", std::ios::in | std::ios::out);
    if(!leaderidFile) {
        printf("No leaderid.txt found. Please create one in build.\n");
        return 0;
    }
    if(argc == 2){
        key = argv[1];
        string val;
        int err = client->Get(key, val);
        return err;
    }
    else if(argc == 3){
        key = argv[1];
        value = argv[2];
        int err = client->Put(key, value);
        return err;
    }else{
        printf("Usage:\nTo get value from db:\n ./db_client <key> \nTo put key value pair in db:\n ./db_client <key> <value>\n");
    }
    leaderidFile.close();

    return 0;
}