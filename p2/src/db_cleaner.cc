#include <iostream>
#include <string>
#include <assert.h>
#include "leveldb/db.h"

using namespace std;

int main(int argc, char **argv)
{
    int serverId = atoi(argv[1]);
    leveldb::DB *pmetadata;
    leveldb::DB *plogs;
    leveldb::Options options;
    leveldb::Status status;
    leveldb::Status pmetadata_status = leveldb::DB::Open(options, "/tmp/pmetadata" + to_string(serverId), &pmetadata);
    leveldb::Status plogs_status = leveldb::DB::Open(options, "/tmp/plogs" + to_string(serverId), &plogs);
    if(!pmetadata_status.ok()) std::cerr << pmetadata_status.ToString() << endl;
    assert(pmetadata_status.ok());

    string key = "1";
    string term = "currentTerm";
    string value = "";
    // read
    status = plogs->Get(leveldb::ReadOptions(), key, &value);
    assert(status.ok());
    cout << value << endl;
    // delete
    status = pmetadata->Delete(leveldb::WriteOptions(), term);
    assert(status.ok());
    // // delete
    // status = plogs->Delete(leveldb::WriteOptions(), key);
    // assert(status.ok());
    key = "2";
    // read
    status = plogs->Get(leveldb::ReadOptions(), key, &value);
    assert(status.ok());
    cout << value << endl;

    //close
    delete pmetadata;
    delete plogs;
    
    return 0;
}
