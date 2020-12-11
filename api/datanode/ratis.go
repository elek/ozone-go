package datanode

import (
	"encoding/binary"
	"github.com/elek/ozone-go/api/proto/datanode"
	"github.com/elek/ozone-go/api/proto/ratis"
	protobuf "github.com/golang/protobuf/proto"
)

func (dnClient *DatanodeClient) sendRatisDatanodeCommand(proto datanode.ContainerCommandRequestProto) (datanode.ContainerCommandResponseProto, error) {
	group := ratis.RaftGroupIdProto{
		Id: make([]byte, 0), //TODO
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

	err := (*dnClient.ratisClient).Send(&request)
	if err != nil {
		return ratis.RaftClientReplyProto{}, err
	}
	resp := <-dnClient.ratisReceiver
	return resp, err
}
