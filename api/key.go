package api

import (
	"github.com/elek/ozone-go/api/common"
	"github.com/elek/ozone-go/api/datanode"
	"io"
)

func (ozoneClient *OzoneClient) ListKeys(volume string, bucket string) ([]common.Key, error) {
	return ozoneClient.omClient.ListKeys(volume, bucket)

}

func (ozoneClient *OzoneClient) ListKeysPrefix(volume string, bucket string, prefix string) ([]common.Key, error) {
	return ozoneClient.omClient.ListKeysPrefix(volume, bucket, prefix)

}

func (ozoneClient *OzoneClient) InfoKey(volume string, bucket string, key string) (common.Key, error) {
	return ozoneClient.omClient.GetKey(volume, bucket, key)
}

func (ozoneClient *OzoneClient) GetKey(volume string, bucket string, key string, destination io.Writer) (common.Key, error) {
	keyInfo, err := ozoneClient.omClient.GetKey(volume, bucket, key)
	if err != nil {
		return common.Key{}, err
	}

	location := keyInfo.Locations[0]
	pipeline := location.Pipeline

	dnClient, err := datanode.CreateDatanodeClient(pipeline)
	chunks, err := dnClient.GetBlock(location.BlockID)
	if err != nil {
		return keyInfo, err
	}
	for _, chunk := range chunks {
		data, err := dnClient.ReadChunk(location.BlockID, chunk)
		if err != nil {
			return keyInfo, err
		}
		destination.Write(data)
	}
	return keyInfo, nil
}

func (ozoneClient *OzoneClient) PutKey(volume string, bucket string, key string, destination io.Writer) (common.Key, error) {
	keyInfo, err := ozoneClient.omClient.GetKey(volume, bucket, key)
	if err != nil {
		return common.Key{}, err
	}

	location := keyInfo.Locations[0]
	pipeline := location.Pipeline

	dnClient, err := datanode.CreateDatanodeClient(pipeline)
	chunks, err := dnClient.GetBlock(location.BlockID)
	if err != nil {
		return keyInfo, err
	}
	for _, chunk := range chunks {
		data, err := dnClient.ReadChunk(location.BlockID, chunk)
		if err != nil {
			return keyInfo, err
		}
		destination.Write(data)
	}
	return keyInfo, nil
}
