package om

import (
	ozone_proto "github.com/elek/ozone-go/api/proto/ozone"
)

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

	_, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return err
	}
	return nil
}
