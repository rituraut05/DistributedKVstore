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
    if(!plogs_status.ok()) std::cerr << plogs_status.ToString() << endl;
    assert(plogs_status.ok());
    
    // delete
    status = pmetadata->Delete(leveldb::WriteOptions(), "currentTerm");
    assert(status.ok());

    status = pmetadata->Delete(leveldb::WriteOptions(), "lastApplied");
    assert(status.ok());

    status = pmetadata->Delete(leveldb::WriteOptions(), "votedFor");
    assert(status.ok());

    int i = 1;
    while(true) {
        string logstr = "";
        leveldb::Status s = plogs->Get(leveldb::ReadOptions(), to_string(i), &logstr);
        if(s.ok()) {
            printf("Erasing log index %d\n", i);
            s = plogs->Delete(leveldb::WriteOptions(), to_string(i));
            i++;
        } else {
            break;
        }
    }

    // read
    string value = "";
    status = plogs->Get(leveldb::ReadOptions(), "1", &value);
    assert(status.ok());
    cout << "1 = " << value << endl;

    //close
    delete pmetadata;
    delete plogs;
    
    return 0;
}
