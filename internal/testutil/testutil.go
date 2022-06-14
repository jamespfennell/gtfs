package testutil

import (
	"testing"

	"github.com/jamespfennell/gtfs"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
	"google.golang.org/protobuf/proto"
)

func MustParse(t *testing.T, header *gtfsrt.FeedHeader, entities []*gtfsrt.FeedEntity, opts *gtfs.ParseRealtimeOptions) *gtfs.Realtime {
	if header == nil {
		v := "2.0"
		header = &gtfsrt.FeedHeader{
			GtfsRealtimeVersion: &v,
		}
	}
	message := gtfsrt.FeedMessage{
		Header: header,
		Entity: entities,
	}
	b, err := proto.Marshal(&message)
	if err != nil {
		t.Fatalf("failed to marshal GTFS-RT message: %s", err)
	}
	result, err := gtfs.ParseRealtime(b, opts)
	if err != nil {
		t.Errorf("failed to parse GTFS-RT message: %s", err)
	}
	return result
}
