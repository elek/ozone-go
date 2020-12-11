package datanode

import (
	"fmt"
	dnapi "github.com/elek/ozone-go/api/proto/datanode"
	"io"
)

func (dnClient *DatanodeClient) sendStandaloneDatanodeCommand(proto dnapi.ContainerCommandRequestProto) (dnapi.ContainerCommandResponseProto, error) {
	err := (*dnClient.standaloneClient).Send(&proto)
	if err != nil {
		return dnapi.ContainerCommandResponseProto{}, err
	}
	resp := <-dnClient.standaloneReceiver
	return resp, err
}

func (dnClient *DatanodeClient) StandaloneReceive() {
	for {
		proto, err := (*dnClient.standaloneClient).Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		dnClient.standaloneReceiver <- *proto
	}
}
