package om

import (
	"github.com/elek/ozone-go/api/common"
	ozone_proto "github.com/elek/ozone-go/api/proto/ozone"
)

func (om *OmClient) ListVolumes() ([]common.Volume, error) {
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

	volumes := make([]common.Volume, 0)
	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return nil, err
	}
	for _, volProto := range resp.GetListVolumeResponse().GetVolumeInfo() {
		volumes = append(volumes, common.Volume{Name: *volProto.Volume})
	}
	return volumes, nil
}

func (om *OmClient) CreateVolume(name string) error {
	onegig := uint64(1024 * 1024 * 1024)
	volumeInfo := ozone_proto.VolumeInfo{
		AdminName:    ptr("admin"),
		OwnerName:    ptr("admin"),
		Volume:       ptr(name),
		QuotaInBytes: &onegig,
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

	_, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return err
	}
	return nil
}
