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
		return common.Key{}, errors.New("Key location doesn't have any datanode for key " + volume + "/" + bucket + "/" + key)
	}
	for _, location := range keyInfo.KeyLocationList[0].KeyLocations {
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
		dnClient.Close()
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
	if err != nil {
		return common.Key{}, err
	}

	chunkSize := 4096
	buffer := make([]byte, chunkSize)

	chunks := make([]*dnproto.ChunkInfo, 0)
	keySize := uint64(0)

	locations := make([]*omproto.KeyLocation, 0)

	blockId := ConvertBlockId(location.BlockID)
	eof := false

	for ; ; {
		blockOffset := uint64(0)
		for i := 0; i < 64; i++ {
			count, err := source.Read(buffer)
			if err == io.EOF {
				eof = true
			} else if err != nil {
				return common.Key{}, err
			}
			if count > 0 {
				chunk, err := dnClient.CreateAndWriteChunk(blockId, blockOffset, buffer[0:count], uint64(count))
				if err != nil {
					return common.Key{}, err
				}
				blockOffset += uint64(count)
				keySize += uint64(count)
				chunks = append(chunks, &chunk)
			}
			if eof {
				break
			}
		}

		err = dnClient.PutBlock(blockId, chunks)
		if err != nil {
			return common.Key{}, err
		}
		if eof {
			break
		}

		//get new block and reset counters

		nextBlockResponse, err := ozoneClient.omClient.AllocateBlock(volume, bucket, key, createKey.ID)
		if err != nil {
			return common.Key{}, err
		}

		dnClient.Close()
		location = nextBlockResponse.KeyLocation
		pipeline = location.Pipeline
		dnClient, err = datanode.CreateDatanodeClient(pipeline)
		if err != nil {
			return common.Key{}, err
		}
		blockId = ConvertBlockId(location.BlockID)
		blockOffset = 0
		chunks = make([]*dnproto.ChunkInfo, 0)

	}
	zero := uint64(0)
	locations = append(locations, &omproto.KeyLocation{
		BlockID:  location.BlockID,
		Pipeline: location.Pipeline,
		Length:   &keySize,
		Offset:   &zero,
	})

	ozoneClient.omClient.CommitKey(volume, bucket, key, createKey.ID, locations, keySize)
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
