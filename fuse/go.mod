module github.com/apache/ozone-go/lib

go 1.13

require (
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/apache/ozone-go/api v0.0.0-20201212100630-cee64fa835db
	github.com/hanwen/go-fuse v1.0.0
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/urfave/cli v1.22.5
)

replace github.com/apache/ozone-go/api => ../api
