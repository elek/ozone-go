package om

import (
	"errors"
	"github.com/elek/ozone-go/api/common"
	"github.com/elek/ozone-go/api/proto/hdds"
	ozone_proto "github.com/elek/ozone-go/api/proto/ozone"
	"github.com/hortonworks/gohadoop"
	hadoop_ipc_client "github.com/hortonworks/gohadoop/hadoop_common/ipc/client"
	uuid "github.com/nu7hatch/gouuid"
	"net"
	"strconv"
)

var OM_PROTOCOL = "org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol"

type OmClient struct {
	OmHost   string
	client   *hadoop_ipc_client.Client
	clientId string
}

func CreateOmClient(omhost string) OmClient {
	clientId, _ := uuid.NewV4()
	ugi, _ := gohadoop.CreateSimpleUGIProto()
	c := &hadoop_ipc_client.Client{
		ClientId:      clientId,
		Ugi:           ugi,
		ServerAddress: net.JoinHostPort(omhost, strconv.Itoa(9862))}

	return OmClient{
		OmHost: omhost,
		client: c,
	}
}

func (om *OmClient) GetKey(volume string, bucket string, key string) (common.Key, error) {

	keyArgs := &ozone_proto.KeyArgs{
		VolumeName: &volume,
		BucketName: &bucket,
		KeyName:    &key,
	}
	req := ozone_proto.LookupKeyRequest{
		KeyArgs: keyArgs,
	}

	requestType := ozone_proto.Type_LookupKey
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:          &requestType,
		LookupKeyRequest: &req,
		ClientId:         &om.clientId,
	}

	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return common.Key{}, err
	}
	keyProto := resp.GetLookupKeyResponse().GetKeyInfo()

	return KeyFromProto(keyProto), nil
}

func KeyFromProto(keyProto *ozone_proto.KeyInfo) common.Key {
	replicationType := common.ReplicationType(*keyProto.Type)
	var locations []common.KeyLocation
	if len(keyProto.KeyLocationList) > 0 {
		locations = make([]common.KeyLocation, len(keyProto.KeyLocationList[0].KeyLocations))
		for i, locationProto := range keyProto.KeyLocationList[0].KeyLocations {
			locations[i] = common.KeyLocation{
				Length:   *locationProto.Length,
				Offset:   *locationProto.Offset,
				BlockID:  common.BlockID{ContainerId: *locationProto.BlockID.ContainerBlockID.ContainerID, LocalId: *locationProto.BlockID.ContainerBlockID.LocalID},
				Pipeline: pipelineFromProto(*locationProto.Pipeline),
			}
		}
	} else {
		locations = make([]common.KeyLocation, 0)
	}
	result := common.Key{
		Name:        *keyProto.KeyName,
		Replication: replicationType,
		Locations:   locations,
	}
	return result
}

func (om *OmClient) ListKeys(volume string, bucket string) ([]common.Key, error) {
	return om.ListKeysPrefix(volume, bucket, "")
}

func (om *OmClient) ListKeysPrefix(volume string, bucket string, prefix string) ([]common.Key, error) {

	req := ozone_proto.ListKeysRequest{
		VolumeName: &volume,
		BucketName: &bucket,
		Prefix:     ptr(prefix),
		Count:      ptri(1000),
	}

	listKeys := ozone_proto.Type_ListKeys
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:         &listKeys,
		ListKeysRequest: &req,
		ClientId:        &om.clientId,
	}

	keys := make([]common.Key, 0)
	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return nil, err
	}
	for _, keyProto := range resp.GetListKeysResponse().GetKeyInfo() {
		keys = append(keys, KeyFromProto(keyProto))
	}
	return keys, nil
}

func pipelineFromProto(pipeline hdds.Pipeline) common.Pipeline {
	datanodes := make([]common.DatanodeDetails, 0)
	for _, datanodeProto := range pipeline.Members {
		datanodes = append(datanodes, common.DatanodeDetails{
			ID:      datanodeProto.GetUuid(),
			Host:    datanodeProto.GetHostName(),
			Ip:      datanodeProto.GetIpAddress(),
			RpcPort: getRpcPort(datanodeProto.GetPorts()),
		})
	}
	return common.Pipeline{
		ID:      *pipeline.Id.Id,
		Members: datanodes,
	}
}

func getRpcPort(ports []*hdds.Port) uint32 {
	for _, port := range ports {
		if port.GetName() == "RATIS" {
			return port.GetValue()
		}
	}
	return 0
}

func ptri(i int32) *int32 {
	return &i
}


func ptr(s string) *string {
	return &s
}

func (om *OmClient) submitRequest(request *ozone_proto.OMRequest, ) (*ozone_proto.OMResponse, error) {
	wrapperResponse := ozone_proto.OMResponse{}
	err := om.client.Call(gohadoop.GetCalleeRPCRequestHeaderProto(&OM_PROTOCOL), request, &wrapperResponse)
	if err != nil {
		return nil, err
	}
	if *wrapperResponse.Status != ozone_proto.Status_OK {
		return nil, errors.New("Error on calling OM " + wrapperResponse.Status.String() + " " + *wrapperResponse.Message)
	}
	return &wrapperResponse, nil
}

