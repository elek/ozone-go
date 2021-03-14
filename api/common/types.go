package common

type ReplicationType int

const (
	RATIS      ReplicationType = 1
	STANDALONE ReplicationType = 2
)

type Volume struct {
	Name  string
	Owner string
}

type Key struct {
	Name        string
	Replication ReplicationType
}

type Bucket struct {
	Name       string
	VolumeName string
}
