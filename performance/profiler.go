package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"

	"github.com/jamespfennell/gtfs"
)

var out = flag.String("out", "gtfs_package_profile.pb.gz", "file path to output the profile to")

func main() {
	if err := run(); err != nil {
		fmt.Println("failed:", err)
		os.Exit(1)
	}
}

func run() error {
	flag.Parse()
	gtfsFiles := flag.Args()
	var gtfsBytes [][]byte
	for _, gtfsFile := range gtfsFiles {
		b, err := os.ReadFile(gtfsFile)
		if err != nil {
			return err
		}
		gtfsBytes = append(gtfsBytes, b)
	}

	fmt.Println("starting profile")
	var profile bytes.Buffer
	pprof.StartCPUProfile(&profile)
	for i, in := range gtfsBytes {
		fmt.Printf("parsing file %d/%d\n", i+1, len(gtfsBytes))
		_, err := gtfs.ParseStatic(in, gtfs.ParseStaticOptions{})
		if err != nil {
			return err
		}
	}
	pprof.StopCPUProfile()

	fmt.Println("writing profile to", *out)
	os.WriteFile(*out, profile.Bytes(), 0644)
	return nil
}
