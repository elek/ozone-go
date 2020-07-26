package api

import "github.com/elek/ozone-go/api/common"

func (ozoneClient *OzoneClient) ListVolumes() ([]common.Volume, error) {
	return ozoneClient.omClient.ListVolumes()
}

func (ozoneClient *OzoneClient) CreateVolume(name string) error {
	return ozoneClient.omClient.CreateVolume(name)
}
