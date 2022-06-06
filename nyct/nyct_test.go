package nyct_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/jamespfennell/gtfs"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
	"google.golang.org/protobuf/proto"
)

const tripID1 = "tripID1"
const track1 = "track1"
const track2 = "track2"

func TestGetTrack(t *testing.T) {
	testCases := []struct {
		HasExtension   bool
		ScheduledTrack *string
		ActualTrack    *string
		ExpectedTrack  *string
	}{
		{
			HasExtension: false,
		},
		{
			HasExtension:   true,
			ScheduledTrack: ptr(track1),
			ActualTrack:    nil,
			ExpectedTrack:  ptr(track1),
		},
		{
			HasExtension:   true,
			ScheduledTrack: nil,
			ActualTrack:    nil,
			ExpectedTrack:  nil,
		},
		{
			HasExtension:   true,
			ScheduledTrack: nil,
			ActualTrack:    ptr(track1),
			ExpectedTrack:  ptr(track1),
		},
		{
			HasExtension:   true,
			ScheduledTrack: ptr(track1),
			ActualTrack:    ptr(track2),
			ExpectedTrack:  ptr(track2),
		},
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			stopTimeUpdate := gtfsrt.TripUpdate_StopTimeUpdate{}
			if testCase.HasExtension {
				proto.SetExtension(&stopTimeUpdate, gtfsrt.E_NyctStopTimeUpdate, &gtfsrt.NyctStopTimeUpdate{
					ScheduledTrack: testCase.ScheduledTrack,
					ActualTrack:    testCase.ActualTrack,
				})
			}
			message := gtfsrt.FeedMessage{
				Header: &gtfsrt.FeedHeader{
					GtfsRealtimeVersion: ptr("2.0"),
				},
				Entity: []*gtfsrt.FeedEntity{
					{
						Id: ptr("1"),
						TripUpdate: &gtfsrt.TripUpdate{
							Trip: &gtfsrt.TripDescriptor{
								TripId: ptr(tripID1),
							},
							StopTimeUpdate: []*gtfsrt.TripUpdate_StopTimeUpdate{&stopTimeUpdate},
						},
					},
				},
			}
			b, err := proto.Marshal(&message)
			if err != nil {
				t.Fatalf("Failed to marshal message: %s", err)
			}

			result, err := gtfs.ParseRealtime(b, &gtfs.ParseRealtimeOptions{
				UseNyctExtension: true,
			})
			if err != nil {
				t.Errorf("unexpected error in ParseRealtime: %s", err)
			}
			expected := []gtfs.Trip{
				{
					ID: gtfs.TripID{
						ID: tripID1,
					},
					StopTimeUpdates: []gtfs.StopTimeUpdate{
						{
							NyctTrack: testCase.ExpectedTrack,
						},
					},
					IsEntityInMessage: true,
				},
			}
			if !reflect.DeepEqual(result.Trips, expected) {
				t.Errorf("actual:\n%+v\n!= expected:\n%+v", result.Trips, expected)
			}
		})
	}
}

func TestFilterStaleUnassignedTrips(t *testing.T) {
	feedTime := uint64(1000)
	beforeFeedTime := int64(500)
	afterFeedTime := int64(1500)
	testCases := []struct {
		FirstStopTime    *gtfsrt.TripUpdate_StopTimeUpdate
		ExpectedNumTrips int
	}{
		{
			FirstStopTime: &gtfsrt.TripUpdate_StopTimeUpdate{
				Departure: &gtfsrt.TripUpdate_StopTimeEvent{
					Time: &beforeFeedTime,
				},
			},
			ExpectedNumTrips: 0,
		},
		{
			FirstStopTime: &gtfsrt.TripUpdate_StopTimeUpdate{
				Departure: &gtfsrt.TripUpdate_StopTimeEvent{
					Time: &afterFeedTime,
				},
			},
			ExpectedNumTrips: 1,
		},
		{
			FirstStopTime: &gtfsrt.TripUpdate_StopTimeUpdate{
				Arrival: &gtfsrt.TripUpdate_StopTimeEvent{
					Time: &beforeFeedTime,
				},
			},
			ExpectedNumTrips: 0,
		},
		{
			FirstStopTime: &gtfsrt.TripUpdate_StopTimeUpdate{
				Arrival: &gtfsrt.TripUpdate_StopTimeEvent{
					Time: &afterFeedTime,
				},
			},
			ExpectedNumTrips: 1,
		},
		{
			FirstStopTime:    &gtfsrt.TripUpdate_StopTimeUpdate{},
			ExpectedNumTrips: 0,
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			message := gtfsrt.FeedMessage{
				Header: &gtfsrt.FeedHeader{
					GtfsRealtimeVersion: ptr("2.0"),
					Timestamp:           &feedTime,
				},
				Entity: []*gtfsrt.FeedEntity{
					{
						Id: ptr("1"),
						TripUpdate: &gtfsrt.TripUpdate{
							Trip:           &gtfsrt.TripDescriptor{},
							StopTimeUpdate: []*gtfsrt.TripUpdate_StopTimeUpdate{tc.FirstStopTime},
						},
					},
				},
			}
			b, err := proto.Marshal(&message)
			if err != nil {
				t.Fatalf("Failed to marshal message: %s", err)
			}

			result, err := gtfs.ParseRealtime(b, &gtfs.ParseRealtimeOptions{
				UseNyctExtension:               true,
				NyctFilterStaleUnassignedTrips: true,
			})
			if err != nil {
				t.Errorf("unexpected error in ParseRealtime: %s", err)
			}
			if len(result.Trips) != tc.ExpectedNumTrips {
				t.Errorf("len(result.Trips)=%d, wanted %d", len(result.Trips), tc.ExpectedNumTrips)
			}
		})
	}
}

func ptr(s string) *string {
	return &s
}
