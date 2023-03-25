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

#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#ifdef BAZEL_BUILD
#include "afs.grpc.pb.h"
#else
#include "afs.grpc.pb.h"
#endif

#define CHUNK_SIZE 4096

namespace fs = std::filesystem;

using afs::PingMessage;
using afs::Timestamp;
using fs::path;
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

static const string TEMP_FILE_EXT = ".afs_tmp";
const std::vector<string> atomicFilesGroup = {};


class AFSImpl final : public FileSystemService::Service
{

public:
    explicit AFSImpl(path root) : root(root) {}

    Status Ping(ServerContext *context, const PingMessage *req, PingMessage *resp) override
    {
        return Status::OK;
    }
};

void RunServer(path root)
{
    std::string server_address("0.0.0.0:50057");
    AFSImpl service(root);

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        fprintf(stdout, "Usage: ./afs_server root_folder\n");
        return -1;
    }

    auto root = fs::canonical(argv[1]);
    RunServer(root);
    return 0;
}
