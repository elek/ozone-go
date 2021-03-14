package api

import "github.com/elek/ozone-go/api/common"

func (ozoneClient *OzoneClient) ListVolumes() ([]common.Volume, error) {
	return ozoneClient.OmClient.ListVolumes()
}

func (ozoneClient *OzoneClient) CreateVolume(name string) error {
	return ozoneClient.OmClient.CreateVolume(name)
}

func (ozoneClient *OzoneClient) GetVolume(name string) (common.Volume, error) {
	return ozoneClient.OmClient.GetVolume(name)
}
