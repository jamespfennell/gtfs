package main

import (
	"fmt"
	"os"
	"time"

	"github.com/jamespfennell/gtfs"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "GTFS parser",
		Usage: "parse GTFS static and realtime feeds",
		Commands: []*cli.Command{
			{
				Name:  "static",
				Usage: "parse a GTFS static message",
				Action: func(*cli.Context) error {
					fmt.Print("static")
					return nil
				},
			},
			{
				Name:  "realtime",
				Usage: "parse a GTFS realtime message",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "verbose",
						Aliases: []string{"v"},
						Usage:   "print additional data about each trip and vehicle",
					},
					&cli.BoolFlag{
						Name:  "nyct",
						Usage: "use the New York City Transit GTFS realtime extension",
					},
				},
				ArgsUsage: "path",
				Action: func(ctx *cli.Context) error {
					args := ctx.Args()
					if args.Len() == 0 {
						return fmt.Errorf("a path to the GTFS realtime message was not provided")
					}
					path := ctx.Args().First()
					b, err := os.ReadFile(path)
					if err != nil {
						return fmt.Errorf("failed to read file %s: %w", path, err)
					}

					opts := gtfs.ParseRealtimeOptions{}
					if ctx.Bool("nyct") {
						opts.UseNyctExtension = true
						americaNewYorkTimezone, err := time.LoadLocation("America/New_York")
						if err == nil {
							opts.Timezone = americaNewYorkTimezone
						}
					}
					realtime, err := gtfs.ParseRealtime(b, &opts)
					if err != nil {
						return fmt.Errorf("failed to parse message: %w", err)
					}
					fmt.Printf("%d trips:\n", len(realtime.Trips))
					for _, trip := range realtime.Trips {
						fmt.Printf("- %s\n", trip)
						if trip.Vehicle != nil {
							fmt.Printf("  Vehicle: %s\n", trip.Vehicle.ID)
						} else {
							fmt.Printf("  Vehicle: none\n")
						}
						if ctx.Bool("verbose") {
							for _, stopTimeUpdate := range trip.StopTimeUpdates {
								fmt.Printf("  * %s\n", stopTimeUpdate)
							}
						}
					}
					return nil
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
}
