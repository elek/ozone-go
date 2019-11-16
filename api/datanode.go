package api

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/elek/ozone-go/api/proto/datanode"
	"github.com/elek/ozone-go/api/proto/ratis"
	protobuf "github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"io"
	"strings"
)

type ChunkInfo struct {
	Name   string
	Offset uint64
	Len    uint64
}

type DatanodeClient struct {
	client          *ratis.RaftClientProtocolService_UnorderedClient
	ctx             context.Context
	datanodes       []DatanodeDetails
	currentDatanode DatanodeDetails
	grpcConnection  *grpc.ClientConn
	pipelineId      []byte
	memberIndex     int
	receiver        chan ratis.RaftClientReplyProto
}

func (dnClient *DatanodeClient) connectToNext() error {
	if dnClient.grpcConnection != nil {
		dnClient.grpcConnection.Close()
	}
	dnClient.memberIndex = dnClient.memberIndex + 1
	if dnClient.memberIndex == len(dnClient.datanodes) {
		dnClient.memberIndex = 0
	}
	dnClient.currentDatanode = dnClient.datanodes[dnClient.memberIndex]
	address := dnClient.datanodes[dnClient.memberIndex].HostAndPort()
	println("Connecting to the " + address)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}
	dnClient.receiver = make(chan ratis.RaftClientReplyProto)
	client, err := ratis.NewRaftClientProtocolServiceClient(conn).Unordered(dnClient.ctx)
	if err != nil {
		panic(err)
	}
	dnClient.client = &client
	go dnClient.Receiver()
	return nil
}

func CreateDatanodeClient(pipeline Pipeline) (*DatanodeClient, error) {
	pipelineId, err := hex.DecodeString(strings.ReplaceAll(pipeline.ID, "-", "", ))
	if err != nil {
		return nil, err
	}

	dnClient := &DatanodeClient{
		ctx:         context.Background(),
		pipelineId:  pipelineId,
		datanodes:   pipeline.Members,
		memberIndex: -1,
	}
	err = dnClient.connectToNext()
	if err != nil {
		return nil, err
	}
	return dnClient, nil
}
func (dnClient *DatanodeClient) Receiver() {

	for {
		proto, err := (*dnClient.client).Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		dnClient.receiver <- *proto
	}
}

func (dnClient *DatanodeClient) ReadChunk(id BlockID, info ChunkInfo) ([]byte, error) {
	result := make([]byte, 0)
	blockIdProto := datanode.DatanodeBlockID{
		ContainerID: &id.ContainerId,
		LocalID:     &id.LocalId,
	}

	bpc := uint32(12)
	checksumType := datanode.ChecksumType_NONE
	checksumDataProto := datanode.ChecksumData{
		Type:             &checksumType,
		BytesPerChecksum: &bpc,
	}
	chunkInfoProto := datanode.ChunkInfo{
		ChunkName:    &info.Name,
		Offset:       &info.Offset,
		Len:          &info.Len,
		ChecksumData: &checksumDataProto,
	}
	req := datanode.ReadChunkRequestProto{
		BlockID:   &blockIdProto,
		ChunkData: &chunkInfoProto,
	}
	commandType := datanode.Type_ReadChunk
	proto := datanode.ContainerCommandRequestProto{
		CmdType:      &commandType,
		ReadChunk:    &req,
		ContainerID:  &id.ContainerId,
		DatanodeUuid: &dnClient.currentDatanode.ID,
	}

	resp, err := dnClient.sendDatanodeCommand(proto)
	if err != nil {
		return result, err
	}
	return resp.GetReadChunk().Data, nil
}

func (dnClient *DatanodeClient) GetBlock(id BlockID) ([]ChunkInfo, error) {
	result := make([]ChunkInfo, 0)
	blockIdProto := datanode.DatanodeBlockID{
		ContainerID: &id.ContainerId,
		LocalID:     &id.LocalId,
	}

	req := datanode.GetBlockRequestProto{
		BlockID: &blockIdProto,
	}
	commandType := datanode.Type_GetBlock
	proto := datanode.ContainerCommandRequestProto{
		CmdType:      &commandType,
		GetBlock:     &req,
		ContainerID:  &id.ContainerId,
		DatanodeUuid: &dnClient.currentDatanode.ID,
	}

	resp, err := dnClient.sendDatanodeCommand(proto)
	if err != nil {
		return result, err
	}
	for _, chunkInfo := range resp.GetGetBlock().GetBlockData().Chunks {
		result = append(result, ChunkInfo{
			Name:   chunkInfo.GetChunkName(),
			Offset: chunkInfo.GetOffset(),
			Len:    chunkInfo.GetLen(),
		})
	}
	return result, nil
}

func (dnClient *DatanodeClient) sendDatanodeCommand(proto datanode.ContainerCommandRequestProto) (datanode.ContainerCommandResponseProto, error) {
	group := ratis.RaftGroupIdProto{
		Id: dnClient.pipelineId,
	}
	request := ratis.RaftRpcRequestProto{
		RequestorId: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5},
		ReplyId:     []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5},
		RaftGroupId: &group,
		CallId:      12,
	}
	bytes, err := protobuf.Marshal(&proto)
	if err != nil {
		return datanode.ContainerCommandResponseProto{}, err
	}

	lengthHeader := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthHeader, uint32(len(bytes)))

	message := ratis.ClientMessageEntryProto{
		Content: append(lengthHeader, bytes...),
	}
	readRequestType := ratis.ReadRequestTypeProto{}
	readType := ratis.RaftClientRequestProto_Read{
		Read: &readRequestType,
	}
	raft := ratis.RaftClientRequestProto{
		RpcRequest: &request,
		Message:    &message,
		Type:       &readType,
	}
	resp, err := dnClient.sendRatisMessage(raft)
	if err != nil {
		return datanode.ContainerCommandResponseProto{}, err
	}

	containerResponse := datanode.ContainerCommandResponseProto{}
	err = protobuf.Unmarshal(resp.Message.Content, &containerResponse)
	if err != nil {
		return containerResponse, err
	}
	return containerResponse, nil
}
func (dnClient *DatanodeClient) sendRatisMessage(request ratis.RaftClientRequestProto) (ratis.RaftClientReplyProto, error) {
	resp, err := dnClient.sendRatisMessageToServer(request)
	if err != nil {
		return ratis.RaftClientReplyProto{}, err
	}
	if resp.GetNotLeaderException() != nil {
		err = dnClient.connectToNext()
		if err != nil {
			return ratis.RaftClientReplyProto{}, err
		}
		resp, err = dnClient.sendRatisMessageToServer(request)
		if err != nil {
			return ratis.RaftClientReplyProto{}, err
		}
	}
	if resp.GetNotLeaderException() != nil {
		err = dnClient.connectToNext()
		if err != nil {
			return ratis.RaftClientReplyProto{}, err
		}
		resp, err = dnClient.sendRatisMessageToServer(request)
		if err != nil {
			return ratis.RaftClientReplyProto{}, err
		}
	}
	return resp, nil
}

func (dnClient *DatanodeClient) sendRatisMessageToServer(request ratis.RaftClientRequestProto) (ratis.RaftClientReplyProto, error) {

	err := (*dnClient.client).Send(&request)
	if err != nil {
		return ratis.RaftClientReplyProto{}, err
	}
	resp := <-dnClient.receiver
	return resp, err
}
