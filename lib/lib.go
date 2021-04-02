package main

import (
	"C"
	"encoding/json"
	"github.com/apache/ozone-go/api/om"
	"math/rand"
)

func main() {

}

var connections = make(map[C.int]*om.OmClient)

func GetKey(omhost *C.char, volume *C.char, bucket *C.char, key *C.char) {
	println("Getting key")
	omClient := om.CreateOmClient(C.GoString(omhost))
	println("Connected to host " + C.GoString(omhost))
	k, err := omClient.GetKey(C.GoString(volume), C.GoString(bucket), C.GoString(key))
	if err != nil {
		panic(err)
	}

	out, err := json.MarshalIndent(k, "", "   ")
	if err != nil {
		panic(err)
	}

	println(string(out))
}

//export CreateOmClient
func CreateOmClient(omhost *C.char) C.int {
	client := om.CreateOmClient(C.GoString(omhost))

	identifier := C.int(rand.Int63())
	connections[identifier] = &client
	return identifier
}

//export PrintKey
func PrintKey(identifier C.int, volume *C.char, bucket *C.char, key *C.char) {
	omClient := connections[identifier]
	println(omClient)
	k, err := omClient.GetKey(C.GoString(volume), C.GoString(bucket), C.GoString(key))
	if err != nil {
		panic(err)
	}
	out, err := json.MarshalIndent(k, "", "   ")
	if err != nil {
		panic(err)
	}

	println(string(out))
}
