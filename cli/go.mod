module github.com/elek/ozone-go

go 1.13

replace github.com/elek/ozone-go/api => ../api

require (
	github.com/elek/ozone-go/api v0.0.0-20180911220305-26e67e76b6c3
	github.com/urfave/cli v1.22.5
	google.golang.org/protobuf v1.25.0 // indirect
)
