package api

func (ozoneClient *OzoneClient) CreateBucket(volume string, bucket string) error {
	return ozoneClient.omClient.CreateBucket(volume, bucket)
}
