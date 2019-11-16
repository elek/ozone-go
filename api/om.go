package api

import (
	"errors"
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

type ReplicationType int

const (
	RATIS      ReplicationType = 1
	STANDALONE ReplicationType = 2
)

type Volume struct {
	Name string
}

type Key struct {
	Name        string
	Replication ReplicationType
	Locations   []KeyLocation
}
type BlockID struct {
	ContainerId int64
	LocalId     int64
}

type DatanodeDetails struct {
	ID      string
	Host    string
	Ip      string
	RpcPort uint32
}

type Pipeline struct {
	ID      string
	Members []DatanodeDetails
}
type KeyLocation struct {
	Length   uint64
	Offset   uint64
	BlockID  BlockID
	Pipeline Pipeline
}

func (dn *DatanodeDetails) HostAndPort() string {
	return dn.Ip + ":" + strconv.Itoa(int(dn.RpcPort))
}

func (om *OmClient) GetKey(volume string, bucket string, key string) (Key, error) {

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

	resp, err := om.SubmitRequest(&wrapperRequest)
	if err != nil {
		return Key{}, err
	}
	keyProto := resp.GetLookupKeyResponse().GetKeyInfo()

	return KeyFromProto(keyProto), nil
}

func KeyFromProto(keyProto *ozone_proto.KeyInfo) Key {
	replicationType := ReplicationType(*keyProto.Type)
	var locations []KeyLocation
	if len(keyProto.KeyLocationList) > 0 {
		locations = make([]KeyLocation, len(keyProto.KeyLocationList[0].KeyLocations))
		for i, locationProto := range keyProto.KeyLocationList[0].KeyLocations {
			locations[i] = KeyLocation{
				Length:   *locationProto.Length,
				Offset:   *locationProto.Offset,
				BlockID:  BlockID{ContainerId: *locationProto.BlockID.ContainerBlockID.ContainerID, LocalId: *locationProto.BlockID.ContainerBlockID.LocalID},
				Pipeline: pipelineFromProto(*locationProto.Pipeline),
			}
		}
	} else {
		locations = make([]KeyLocation, 0)
	}
	result := Key{
		Name:        *keyProto.KeyName,
		Replication: replicationType,
		Locations:   locations,
	}
	return result
}
func (om *OmClient) ListKeys(volume string, bucket string) ([]Key, error) {

	req := ozone_proto.ListKeysRequest{
		VolumeName: &volume,
		BucketName: &bucket,
		Prefix:     ptr(""),
		Count:      ptri(1000),
	}

	listKeys := ozone_proto.Type_ListKeys
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:         &listKeys,
		ListKeysRequest: &req,
		ClientId:        &om.clientId,
	}

	keys := make([]Key, 0)
	resp, err := om.SubmitRequest(&wrapperRequest)
	if err != nil {
		return nil, err
	}
	for _, keyProto := range resp.GetListKeysResponse().GetKeyInfo() {
		keys = append(keys, KeyFromProto(keyProto))
	}
	return keys, nil
}

func pipelineFromProto(pipeline hdds.Pipeline) Pipeline {
	datanodes := make([]DatanodeDetails, 0)
	for _, datanodeProto := range pipeline.Members {
		datanodes = append(datanodes, DatanodeDetails{
			ID:      datanodeProto.GetUuid(),
			Host:    datanodeProto.GetHostName(),
			Ip:      datanodeProto.GetIpAddress(),
			RpcPort: getRpcPort(datanodeProto.GetPorts()),
		})
	}
	return Pipeline{
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

func (om *OmClient) CreateBucket(volume string, bucket string) error {
	isVersionEnabled := false
	storageType := ozone_proto.StorageTypeProto_DISK
	bucketInfo := ozone_proto.BucketInfo{
		BucketName:       &bucket,
		VolumeName:       &volume,
		IsVersionEnabled: &isVersionEnabled,
		StorageType:      &storageType,
	}
	req := ozone_proto.CreateBucketRequest{
		BucketInfo: &bucketInfo,
	}

	cmdType := ozone_proto.Type_CreateBucket
	clientId := "goClient"
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:             &cmdType,
		CreateBucketRequest: &req,
		ClientId:            &clientId,
	}

	_, err := om.SubmitRequest(&wrapperRequest)
	if err != nil {
		return err
	}
	return nil
}
func (om *OmClient) CreateVolume(name string) error {
	volumeInfo := ozone_proto.VolumeInfo{
		AdminName: ptr("admin"),
		OwnerName: ptr("admin"),
		Volume:    ptr(name),
	}
	req := ozone_proto.CreateVolumeRequest{
		VolumeInfo: &volumeInfo,
	}

	cmdType := ozone_proto.Type_CreateVolume
	clientId := "goClient"
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:             &cmdType,
		CreateVolumeRequest: &req,
		ClientId:            &clientId,
	}

	_, err := om.SubmitRequest(&wrapperRequest)
	if err != nil {
		return err
	}
	return nil
}

func (om *OmClient) ListVolumes() ([]Volume, error) {
	scope := ozone_proto.ListVolumeRequest_USER_VOLUMES
	req := ozone_proto.ListVolumeRequest{
		Scope:    &scope,
		UserName: ptr("admin"),
		Prefix:   ptr(""),
	}

	listKeys := ozone_proto.Type_ListKeys
	clientId := "goClient"
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:           &listKeys,
		ListVolumeRequest: &req,
		ClientId:          &clientId,
	}

	volumes := make([]Volume, 0)
	resp, err := om.SubmitRequest(&wrapperRequest)
	if err != nil {
		return nil, err
	}
	for _, volProto := range resp.GetListVolumeResponse().GetVolumeInfo() {
		volumes = append(volumes, Volume{Name: *volProto.Volume})
	}
	return volumes, nil
}

func ptr(s string) *string {
	return &s
}

func (om *OmClient) SubmitRequest(request *ozone_proto.OMRequest, ) (*ozone_proto.OMResponse, error) {
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
