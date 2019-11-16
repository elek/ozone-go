module github.com/elek/ozone-go

go 1.13

require (
	github.com/elek/ozone-go/api v0.0.0-20180911220305-26e67e76b6c3
	github.com/golang/protobuf v1.3.2
	github.com/urfave/cli v1.22.1
	golang.org/x/net v0.0.0-20180911220305-26e67e76b6c3
	google.golang.org/grpc v1.15.0
)

replace github.com/elek/ozone-go/api => ./api
