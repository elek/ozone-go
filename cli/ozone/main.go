package main

// import "github.com/elek/ozone-go"
import (
	"encoding/json"
	"fmt"
	pkg "github.com/elek/ozone-go/api"
	"github.com/urfave/cli"
	"log"
	"os"
	"strings"
)

var version string
var commit string
var date string

func main() {

	app := cli.NewApp()
	app.Name = "ozone"
	app.Usage = "Ozone command line client"
	app.Description = "Native Ozone command line client"
	app.Version = fmt.Sprintf("%s (%s, %s)", version, commit, date)
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:     "om",
			Required: true,
			Value:    "localhost",
			Usage:    "Host (or host:port) address of the OzoneManager",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:    "volume",
			Aliases: []string{"v", "vol"},
			Usage:   "Ozone volume related operations",
			Flags: []cli.Flag{

			},
			Subcommands: []cli.Command{
				{
					Name:    "list",
					Aliases: []string{"ls"},
					Usage:   "List volumes.",
					Action: func(c *cli.Context) error {
						omClient := pkg.CreateOmClient(c.String("om"))
						volumes, err := omClient.ListVolumes()
						if err != nil {
							return err
						}
						for _, volume := range volumes {
							println(volume.Name)
						}
						return nil
					},
				},
				{
					Name:    "create",
					Aliases: []string{"mk"},
					Usage:   "Create volume.",
					Action: func(c *cli.Context) error {
						omClient := pkg.CreateOmClient(c.GlobalString("om"))
						address := OzoneObjectAddressFromString(c.Args().Get(0))
						err := omClient.CreateVolume(*address.Volume)
						if err != nil {
							return err
						}
						return nil
					},
				},
			},
		},
		{
			Name:    "bucket",
			Aliases: []string{"b"},
			Usage:   "Ozone bucket related operations",
			Flags: []cli.Flag{

			},
			Subcommands: []cli.Command{
				{
					Name:    "create",
					Aliases: []string{"mk"},
					Usage:   "Create bucket.",
					Action: func(c *cli.Context) error {
						omClient := pkg.CreateOmClient(c.GlobalString("om"))
						address := OzoneObjectAddressFromString(c.Args().Get(0))
						err := omClient.CreateBucket(*address.Volume, *address.Bucket)
						if err != nil {
							return err
						}
						return nil
					},
				},
			},
		},
		{
			Name:    "key",
			Aliases: []string{"k"},
			Usage:   "Ozone key related operations",
			Flags: []cli.Flag{

			},
			Subcommands: []cli.Command{
				{
					Name:    "list",
					Aliases: []string{"ls"},
					Usage:   "List keys.",
					Action: func(c *cli.Context) error {
						omClient := pkg.CreateOmClient(c.GlobalString("om"))
						address := OzoneObjectAddressFromString(c.Args().Get(0))
						keys, err := omClient.ListKeys(*address.Volume, *address.Bucket)
						if err != nil {
							return err
						}
						out, err := json.MarshalIndent(keys, "", "   ")
						if err != nil {
							return err
						}

						println(string(out))
						return nil
					},
				},
				{
					Name:    "get",
					Aliases: []string{"show"},
					Usage:   "Show information about one key",
					Action: func(c *cli.Context) error {
						omClient := pkg.CreateOmClient(c.GlobalString("om"))
						address := OzoneObjectAddressFromString(c.Args().Get(0))
						key, err := omClient.GetKey(*address.Volume, *address.Bucket, *address.Key)
						if err != nil {
							return err
						}
						out, err := json.MarshalIndent(key, "", "   ")
						if err != nil {
							return err
						}

						println(string(out))
						return nil
					},
				},
				{
					Name:    "cat",
					Aliases: []string{"c"},
					Usage:   "Show content of a file",
					Action: func(c *cli.Context) error {
						omClient := pkg.CreateOmClient(c.GlobalString("om"))
						address := OzoneObjectAddressFromString(c.Args().Get(0))
						key, err := omClient.GetKey(*address.Volume, *address.Bucket, *address.Key)
						if err != nil {
							return err
						}

						location := key.Locations[0]
						pipeline := location.Pipeline

						dnClient, err := pkg.CreateDatanodeClient(pipeline)
						chunks, err := dnClient.GetBlock(location.BlockID)
						if err != nil {
							return err
						}
						for _, chunk := range chunks {
							data, err := dnClient.ReadChunk(location.BlockID, chunk)
							if err != nil {
								return err
							}
							fmt.Println(string(data))
						}

						return nil
					},
				},
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

type OzoneObjectAddress struct {
	Volume *string
	Bucket *string
	Key    *string
}

func OzoneObjectAddressFromString(get string) OzoneObjectAddress {
	volumeBucketKey := strings.SplitN(get, "/", 3)
	o := OzoneObjectAddress{Volume: &volumeBucketKey[0]}
	if len(volumeBucketKey) > 1 {
		o.Bucket = &volumeBucketKey[1]
	}
	if len(volumeBucketKey) > 2 {
		o.Key = &volumeBucketKey[2]
	}
	return o

}
