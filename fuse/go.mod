module github.com/elek/ozone-go/lib

go 1.13

require (
	github.com/elek/ozone-go/api v0.0.0-20180911220305-26e67e76b6c3
	github.com/hanwen/go-fuse v1.0.0
	github.com/urfave/cli v1.22.1
)

replace github.com/elek/ozone-go/api => ../api
