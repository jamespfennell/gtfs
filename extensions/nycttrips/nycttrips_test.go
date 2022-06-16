package nycttrips_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/jamespfennell/gtfs"
	"github.com/jamespfennell/gtfs/extensions/nycttrips"
	"github.com/jamespfennell/gtfs/internal/testutil"
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

			entities := []*gtfsrt.FeedEntity{
				{
					Id: ptr("1"),
					TripUpdate: &gtfsrt.TripUpdate{
						Trip: &gtfsrt.TripDescriptor{
							TripId: ptr(tripID1),
						},
						StopTimeUpdate: []*gtfsrt.TripUpdate_StopTimeUpdate{&stopTimeUpdate},
					},
				},
			}

			result := testutil.MustParse(t, nil, entities, &gtfs.ParseRealtimeOptions{
				Extension: nycttrips.Extension(nycttrips.ExtensionOpts{
					FilterStaleUnassignedTrips: true,
				}),
			})

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
			tripDescriptor := &gtfsrt.TripDescriptor{}
			proto.SetExtension(tripDescriptor, gtfsrt.E_NyctTripDescriptor, &gtfsrt.NyctTripDescriptor{})
			entities := []*gtfsrt.FeedEntity{
				{
					Id: ptr("1"),
					TripUpdate: &gtfsrt.TripUpdate{
						Trip:           tripDescriptor,
						StopTimeUpdate: []*gtfsrt.TripUpdate_StopTimeUpdate{tc.FirstStopTime},
					},
				},
			}
			header := &gtfsrt.FeedHeader{
				GtfsRealtimeVersion: ptr("2.0"),
				Timestamp:           &feedTime,
			}

			result := testutil.MustParse(t, header, entities, &gtfs.ParseRealtimeOptions{
				Extension: nycttrips.Extension(nycttrips.ExtensionOpts{
					FilterStaleUnassignedTrips: true,
				}),
			})

			if len(result.Trips) != tc.ExpectedNumTrips {
				t.Errorf("len(result.Trips)=%d, wanted %d", len(result.Trips), tc.ExpectedNumTrips)
			}
		})
	}
}

func ptr(s string) *string {
	return &s
}
