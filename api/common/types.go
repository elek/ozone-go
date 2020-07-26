package common

import "strconv"

type ReplicationType int

const (
	RATIS      ReplicationType = 1
	STANDALONE ReplicationType = 2
)

type Volume struct {
	Name string
}

type Key struct {
	Name        string
	Replication ReplicationType
	Locations   []KeyLocation
}
type BlockID struct {
	ContainerId int64
	LocalId     int64
}

type DatanodeDetails struct {
	ID      string
	Host    string
	Ip      string
	RpcPort uint32
}

type Pipeline struct {
	ID      string
	Members []DatanodeDetails
}
type KeyLocation struct {
	Length   uint64
	Offset   uint64
	BlockID  BlockID
	Pipeline Pipeline
}

func (dn *DatanodeDetails) HostAndPort() string {
	return dn.Ip + ":" + strconv.Itoa(int(dn.RpcPort))
}
