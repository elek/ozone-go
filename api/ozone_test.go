package api

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)
import "github.com/stretchr/testify/assert"

func randomName(prefix string) string {
	seq := rand.New(rand.NewSource(time.Now().Unix())).Int31()
	return prefix + fmt.Sprintf("%d", seq)
}

func TestOzoneClientVolumeCreateGet(t *testing.T) {
	client := CreateOzoneClient("localhost")

	volumeName := randomName("vol")
	err := client.CreateVolume(volumeName)
	assert.Nil(t, err)

	vol, err := client.GetVolume(volumeName)
	assert.Nil(t, err)

	assert.Equal(t, volumeName, vol.Name)
}

func TestOzoneClientBucketCreateGet(t *testing.T) {

	client := CreateOzoneClient("localhost")

	//volumeName := "vol1"
	volumeName := randomName("vol")
	bucketName := randomName("bucket")

	err := client.CreateVolume(volumeName)
	assert.Nil(t, err)

	time.Sleep(4 * time.Second)




	err = client.CreateBucket(volumeName, bucketName)
	assert.Nil(t, err)

	bucket, err := client.GetBucket(volumeName, bucketName)
	assert.Nil(t, err)

	assert.Equal(t, bucketName, bucket.Name)
	assert.Equal(t, volumeName, bucket.VolumeName)
}
