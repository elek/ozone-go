package main

import (
	"fmt"
	"github.com/elek/ozone-go/api"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/urfave/cli"
	"log"
	"os"
	"strings"
)

var version string
var commit string
var date string

type OzoneFs struct {
	pathfs.FileSystem
	ozoneClient *api.OzoneClient
	Volume      string
	Bucket      string
}

func (me *OzoneFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if name == "" {
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	}

	key, err := me.ozoneClient.InfoKey(me.Volume, me.Bucket, name)
	if err != nil {
		fmt.Println("Error with getting key: " + name + " " + err.Error())
		return nil, fuse.ENOENT
	}

	if len(key.Locations) > 1 {
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	}

	if len(key.Locations) == 1 {
		return &fuse.Attr{
			Mode: fuse.S_IFREG | 0644, Size: uint64(len(name))}, fuse.OK
	}

	return nil, fuse.ENOENT
}

func (me *OzoneFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {

	keys, err := me.ozoneClient.ListKeysPrefix(me.Volume, me.Bucket, name)
	if err != nil {
		panic(err)
	}
	result := make([]fuse.DirEntry, 0)
	var lastDir = ""
	for _, key := range keys {
		keyName := key.Name
		relative := keyName[len(name):]
		levels := strings.Count(relative, "/")
		if levels > 0 {
			name := relative[0:strings.Index(relative, "/")]
			if name != lastDir {
				entry := fuse.DirEntry{Name: name, Mode: fuse.S_IFDIR}
				result = append(result, entry)
				lastDir = name
			}
		} else {
			entry := fuse.DirEntry{Name: relative, Mode: fuse.S_IFREG}
			result = append(result, entry)
		}

	}
	return result, fuse.OK

}

func (me *OzoneFs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	key, err := me.ozoneClient.InfoKey(me.Volume, me.Bucket, name)
	if err != nil {
		return nil, fuse.EACCES
	}
	return CreateOzoneFile(me.ozoneClient, key), fuse.OK
}


func main() {
	app := cli.NewApp()
	app.Name = "ozone-fuse"
	app.Usage = "Ozone fuse driver"
	app.Description = "FUSE filesystem driver for Apache Hadoop Ozone"
	app.Version = fmt.Sprintf("%s (%s, %s)", version, commit, date)
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:     "om",
			Required: true,
			Value:    "localhost",
			Usage:    "Host (or host:port) address of the OzoneManager",
		},
	}
	app.Action = func(c *cli.Context) error {
		client := api.CreateOzoneClient(c.String("om"))
		mountPoint := "/tmp/ozone"
		fs := &OzoneFs{FileSystem: pathfs.NewDefaultFileSystem(), ozoneClient: client, Volume: "vol1", Bucket: "bucket1"}
		nfs := pathfs.NewPathNodeFs(fs, nil)
		server, _, err := nodefs.MountRoot(mountPoint, nfs.Root(), nil)
		if err != nil {
			log.Fatalf("Mount fail: %v\n", err)
		}
		server.Serve()
		return nil
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
