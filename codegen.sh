#!/usr/bin/env bash
set -ex
PROTOC=/usr/bin/protoc
mkdir -p api/proto/common
mkdir -p api/proto/hdds
mkdir -p api/proto/ozone
mkdir -p api/proto/datanode
mkdir -p api/proto/ratis

$PROTOC -I $(pwd)/proto $(pwd)/proto/Security.proto --go_out=/tmp/
mv /tmp/github.com/apache/ozone-go/api/proto/common/Security.pb.go api/proto/common/

$PROTOC -I $(pwd)/proto $(pwd)/proto/hdds.proto --go_out=/tmp/
mv /tmp/github.com/apache/ozone-go/api/proto/hdds/hdds.pb.go api/proto/hdds/

$PROTOC -I $(pwd)/proto $(pwd)/proto/DatanodeClientProtocol.proto --go_out=/tmp/
mv /tmp/github.com/apache/ozone-go/api/proto/datanode/DatanodeClientProtocol.pb.go api/proto/datanode/


$PROTOC -I $(pwd)/proto $(pwd)/proto/DatanodeClientProtocol.proto --go_out=plugins=grpc:/tmp/
mv /tmp/github.com/apache/ozone-go/api/proto/datanode/DatanodeClientProtocol.pb.go api/proto/datanode/

$PROTOC -I $(pwd)/proto $(pwd)/proto/OmClientProtocol.proto --go_out=/tmp/
mv /tmp/github.com/apache/ozone-go/api/proto/ozone/OmClientProtocol.pb.go api/proto/ozone/


$PROTOC -I $(pwd)/proto $(pwd)/proto/raft.proto --go_out=/tmp/
mv /tmp/github.com/apache/ozone-go/api/proto/ratis/raft.pb.go api/proto/ratis/


$PROTOC -I $(pwd)/proto $(pwd)/proto/ratis-grpc.proto --go_out=plugins=grpc:/tmp/
mv /tmp/github.com/apache/ozone-go/api/proto/ratis/ratis-grpc.pb.go api/proto/ratis/

