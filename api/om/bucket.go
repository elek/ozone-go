package om

import (
	"github.com/elek/ozone-go/api/common"
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
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:             &cmdType,
		CreateBucketRequest: &req,
		ClientId:            &om.clientId,
	}

	_, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return err
	}
	return nil
}

func (om *OmClient) GetBucket(volume string, bucket string) (common.Bucket, error) {
	req := ozone_proto.InfoBucketRequest{
		VolumeName: &volume,
		BucketName: &bucket,
	}

	cmdType := ozone_proto.Type_InfoBucket
	wrapperRequest := ozone_proto.OMRequest{
		CmdType:           &cmdType,
		InfoBucketRequest: &req,
		ClientId:          &om.clientId,
	}

	resp, err := om.submitRequest(&wrapperRequest)
	if err != nil {
		return common.Bucket{}, err
	}
	b := common.Bucket{
		Name:       *resp.InfoBucketResponse.BucketInfo.BucketName,
		VolumeName: *resp.InfoBucketResponse.BucketInfo.VolumeName,
	}
	return b, nil
}

