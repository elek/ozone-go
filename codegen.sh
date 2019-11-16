#!/usr/bin/env bash
PROTOC=/usr/bin/protoc
mkdir -p api/proto/common
mkdir -p api/proto/hdds
mkdir -p api/proto/ozone
mkdir -p api/proto/datanode
mkdir -p api/proto/ratis

$PROTOC -I $(pwd)/proto $(pwd)/proto/Security.proto --go_out=/tmp/
mv /tmp/github.com/elek/ozone-go/api/proto/common/Security.pb.go api/proto/common/

$PROTOC -I $(pwd)/proto $(pwd)/proto/FSProtos.proto --go_out=/tmp/
mv /tmp/github.com/elek/ozone-go/api/proto/common/FSProtos.pb.go api/proto/common/

$PROTOC -I $(pwd)/proto $(pwd)/proto/hdds.proto --go_out=/tmp/
mv /tmp/github.com/elek/ozone-go/api/proto/hdds/hdds.pb.go api/proto/hdds/

$PROTOC -I $(pwd)/proto $(pwd)/proto/ozone.proto --go_out=/tmp/
mv /tmp/github.com/elek/ozone-go/api/proto/ozone/ozone.pb.go api/proto/ozone/

$PROTOC -I $(pwd)/proto $(pwd)/proto/datanode-client.proto --go_out=/tmp/
mv /tmp/github.com/elek/ozone-go/api/proto/datanode/datanode-client.pb.go api/proto/datanode/


$PROTOC -I $(pwd)/proto $(pwd)/proto/raft.proto --go_out=/tmp/
mv /tmp/github.com/elek/ozone-go/api/proto/ratis/raft.pb.go api/proto/ratis/


$PROTOC -I $(pwd)/proto $(pwd)/proto/ratis-grpc.proto --go_out=plugins=grpc:/tmp/
mv /tmp/github.com/elek/ozone-go/api/proto/ratis/ratis-grpc.pb.go api/proto/ratis/


#protoc -I `pwd`/proto/common  `pwd`/proto/common/Security.proto --go_out=`pwd`/proto/common
#protoc -I `pwd`/proto/hdds  `pwd`/proto/hdds/hdds.proto --go_out=`pwd`/proto/hdds
#protoc -I `pwd`/proto/ozone -I `pwd`/proto/common -I `pwd`/proto/hdds `pwd`/proto/ozone/ozone.proto --go_out=`pwd`/proto/ozone
