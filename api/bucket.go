package api

import "github.com/elek/ozone-go/api/common"

func (ozoneClient *OzoneClient) CreateBucket(volume string, bucket string) error {
	return ozoneClient.OmClient.CreateBucket(volume, bucket)
}


func (ozoneClient *OzoneClient) GetBucket(volume string, bucket string) (common.Bucket, error) {
	return ozoneClient.OmClient.GetBucket(volume, bucket)

}