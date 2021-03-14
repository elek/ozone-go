module github.com/elek/ozone-go

go 1.13

replace github.com/elek/ozone-go/api => ../api

require (
	github.com/elek/ozone-go/api v0.0.0-20201212100630-cee64fa835db
	github.com/urfave/cli v1.22.5
	google.golang.org/protobuf v1.25.0 // indirect
)
