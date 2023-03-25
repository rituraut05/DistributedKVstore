#include "afs_client.hh"
#include "wrapper.h"

#include "afs.grpc.pb.h"
#include "afs.pb.h"

extern "C" {
	WFileSystemClient create_FileSystemClient(char* server_addr) {
		return (FileSystemClient*) new FileSystemClient(grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials()));
	}

	int ping_FileSystemClient(WFileSystemClient v, int* i) {
		return ((FileSystemClient*)v)->Ping(i);
	}

	int getFileStat_FileSystemClient(WFileSystemClient v, char* path, struct stat *buf, char* root) {
		return ((FileSystemClient*)v)->GetFileStat(path, buf, root);
	}

	int openUsingStream_FileSystemClient(WFileSystemClient v, char* path, char* root, int flags) {
		return((FileSystemClient*)v)->OpenFileUsingStream(path, root, flags);
	}

	int access_FileSystemClient(WFileSystemClient v, char* path, int mode, char* root) {
		return ((FileSystemClient*)v)->Access(path, mode, root);
	}

	int closeUsingStream_FileSystemClient(WFileSystemClient v, int fd, char* path, char* root) {
		return ((FileSystemClient*)v)->CloseFileUsingStream(fd, path, root);
	}
	
	int makeDir_FileSystemClient(WFileSystemClient v, char* abs_path, char* root, mode_t mode) {
		return ((FileSystemClient*)v)->MakeDir(abs_path, root, mode);
	}

	int removeDir_FileSystemClient(WFileSystemClient v, char* abs_path, char* root) {
                return ((FileSystemClient*)v)->RemoveDir(abs_path, root);
        }

	int readDir_FileSystemClient(WFileSystemClient v, char* abs_path, char* root, void *buf, int* filler) {
                return ((FileSystemClient*)v)->ReadDir(abs_path, root, buf, (fuse_fill_dir_t) filler);
        }
	
	int createFile_FileSystemClient(WFileSystemClient v, char* abs_path, char* root, mode_t mode, int flags) {
			return ((FileSystemClient*)v)->CreateFile(abs_path, root, mode, flags);
	}

	int deleteFile_FileSystemClient(WFileSystemClient v, char* abs_path, char* root) {
		return ((FileSystemClient*)v)->DeleteFile(abs_path, root);
	}
	
	int rename_FileSystemClient(WFileSystemClient v, char* abs_path, char* new_name, char* root) {
                return ((FileSystemClient*)v)->Rename(abs_path, new_name, root);
        }
}
