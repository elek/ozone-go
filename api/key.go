package api

import (
	"errors"
	"github.com/elek/ozone-go/api/common"
	"github.com/elek/ozone-go/api/datanode"
	dnproto "github.com/elek/ozone-go/api/proto/datanode"
	"github.com/elek/ozone-go/api/proto/hdds"
	omproto "github.com/elek/ozone-go/api/proto/ozone"

	"io"
)

func (ozoneClient *OzoneClient) ListKeys(volume string, bucket string) ([]common.Key, error) {

	keys, err := ozoneClient.omClient.ListKeys(volume, bucket)
	if err != nil {
		return make([]common.Key, 0), err
	}

	ret := make([]common.Key, 0)
	for _, r := range keys {
		ret = append(ret, KeyFromProto(r))
	}
	return ret, nil

}

func (ozoneClient *OzoneClient) ListKeysPrefix(volume string, bucket string, prefix string) ([]common.Key, error) {
	keys, err := ozoneClient.omClient.ListKeysPrefix(volume, bucket, prefix)
	if err != nil {
		return make([]common.Key, 0), err
	}

	ret := make([]common.Key, 0)
	for _, r := range keys {
		ret = append(ret, KeyFromProto(r))
	}
	return ret, nil

}

func (ozoneClient *OzoneClient) InfoKey(volume string, bucket string, key string) (common.Key, error) {
	k, err := ozoneClient.omClient.GetKey(volume, bucket, key)
	return KeyFromProto(k), err
}

func (ozoneClient *OzoneClient) GetKey(volume string, bucket string, key string, destination io.Writer) (common.Key, error) {
	keyInfo, err := ozoneClient.omClient.GetKey(volume, bucket, key)
	if err != nil {
		return common.Key{}, err
	}

	if len(keyInfo.KeyLocationList) == 0 {
		return common.Key{}, errors.New("Get key returned with zero key location version " + volume + "/" + bucket + "/" + key)
	}

	if len(keyInfo.KeyLocationList[0].KeyLocations) == 0 {
		return common.Key{}, errors.New("Key locatino doesn't have any datanode for key " + volume + "/" + bucket + "/" + key)
	}
	location := keyInfo.KeyLocationList[0].KeyLocations[0]
	pipeline := location.Pipeline

	dnBlockId := ConvertBlockId(location.BlockID)
	dnClient, err := datanode.CreateDatanodeClient(pipeline)
	chunks, err := dnClient.GetBlock(dnBlockId)
	if err != nil {
		return common.Key{}, err
	}
	for _, chunk := range chunks {
		data, err := dnClient.ReadChunk(dnBlockId, chunk)
		if err != nil {
			return common.Key{}, err
		}
		destination.Write(data)
	}
	return common.Key{}, nil
}

func ConvertBlockId(bid *hdds.BlockID) *dnproto.DatanodeBlockID {
	id := dnproto.DatanodeBlockID{
		ContainerID: bid.ContainerBlockID.ContainerID,
		LocalID:     bid.ContainerBlockID.LocalID,
	}
	return &id
}

func (ozoneClient *OzoneClient) PutKey(volume string, bucket string, key string, source io.Reader) (common.Key, error) {
	createKey, err := ozoneClient.omClient.CreateKey(volume, bucket, key)
	if err != nil {
		return common.Key{}, err
	}

	keyInfo := createKey.KeyInfo
	location := keyInfo.KeyLocationList[0].KeyLocations[0]
	pipeline := location.Pipeline

	dnClient, err := datanode.CreateDatanodeClient(pipeline)

	buffer := make([]byte, 4096)

	count, err := source.Read(buffer)
	if err != nil {
		return common.Key{}, err
	}
	chunks := make([]*dnproto.ChunkInfo, 0)
	size := uint64(0)

	locations := make([]*omproto.KeyLocation, 0)

	if count > 0 {
		blockId := ConvertBlockId(location.BlockID)
		chunk, err := dnClient.CreateAndWriteChunk(blockId, 0, buffer, uint64(count))
		if err != nil {
			return common.Key{}, err
		}
		size += uint64(count)
		chunks = append(chunks, &chunk)
		err = dnClient.PutBlock(blockId, chunks)
		if err != nil {
			return common.Key{}, err
		}
		zero := uint64(0)
		locations = append(locations, &omproto.KeyLocation{
			BlockID:  location.BlockID,
			Pipeline: location.Pipeline,
			Length:   &size,
			Offset:   &zero,
		})
	}

	ozoneClient.omClient.CommitKey(volume, bucket, key, createKey.ID, locations, size)
	return common.Key{}, nil
}

func KeyFromProto(keyProto *omproto.KeyInfo) common.Key {
	replicationType := common.ReplicationType(*keyProto.Type)

	result := common.Key{
		Name:        *keyProto.KeyName,
		Replication: replicationType,
	}
	return result
}
