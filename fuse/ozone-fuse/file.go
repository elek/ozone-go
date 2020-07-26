package main

import (
	"github.com/elek/ozone-go/api"
	"github.com/elek/ozone-go/api/common"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"time"
)

type OzoneFile struct {
	ozoneClient *api.OzoneClient
	key         common.Key
}

func CreateOzoneFile(ozoneClient *api.OzoneClient, key common.Key) nodefs.File {
	return &OzoneFile{
		ozoneClient: ozoneClient,
		key:         key,
	}
}

func (f *OzoneFile) SetInode(*nodefs.Inode) {
}

func (f *OzoneFile) InnerFile() nodefs.File {
	return nil
}

func (f *OzoneFile) String() string {
	return "OzoneFile"
}

func (f *OzoneFile) Read(buf []byte, off int64) (fuse.ReadResult, fuse.Status) {
	return nil, fuse.ENOSYS
}

func (f *OzoneFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	return 0, fuse.ENOSYS
}

func (f *OzoneFile) GetLk(owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) (code fuse.Status) {
	return fuse.ENOSYS
}

func (f *OzoneFile) SetLk(owner uint64, lk *fuse.FileLock, flags uint32) (code fuse.Status) {
	return fuse.ENOSYS
}

func (f *OzoneFile) SetLkw(owner uint64, lk *fuse.FileLock, flags uint32) (code fuse.Status) {
	return fuse.ENOSYS
}

func (f *OzoneFile) Flush() fuse.Status {
	return fuse.OK
}

func (f *OzoneFile) Release() {

}

func (f *OzoneFile) GetAttr(attr *fuse.Attr) fuse.Status {
	return fuse.OK
}

func (f *OzoneFile) Fsync(flags int) (code fuse.Status) {
	return fuse.ENOSYS
}

func (f *OzoneFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	return fuse.ENOSYS
}

func (f *OzoneFile) Truncate(size uint64) fuse.Status {
	return fuse.ENOSYS
}

func (f *OzoneFile) Chown(uid uint32, gid uint32) fuse.Status {
	return fuse.ENOSYS
}

func (f *OzoneFile) Chmod(perms uint32) fuse.Status {
	return fuse.ENOSYS
}

func (f *OzoneFile) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	return fuse.ENOSYS
}
