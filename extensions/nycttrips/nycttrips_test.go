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

func TestFixMTrainPlatformsInBushwick(t *testing.T) {
	testCases := []struct {
		Name         string
		FlagValue    bool
		RouteID      string
		InputStopIDs []string
		WantStopIDs  []string
	}{
		{
			Name:         "Not the M train",
			RouteID:      "J",
			InputStopIDs: []string{"M18N", "M19N"},
			WantStopIDs:  []string{"M18N", "M19N"},
		},
		{
			Name:         "Northbound M train",
			RouteID:      "M",
			InputStopIDs: []string{"M18N", "M19N"},
			WantStopIDs:  []string{"M18S", "M19N"},
		},
		{
			Name:         "Southbound M train",
			RouteID:      "M",
			InputStopIDs: []string{"M18S", "M19S"},
			WantStopIDs:  []string{"M18N", "M19S"},
		},
		{
			Name:         "Gracefully handle invalid stop IDs",
			RouteID:      "M",
			InputStopIDs: []string{"invalidStopID", "i"},
			WantStopIDs:  []string{"invalidStopID", "i"},
		},
		{
			Name:         "Fix disabled",
			FlagValue:    true,
			RouteID:      "M",
			InputStopIDs: []string{"M18S", "M19S"},
			WantStopIDs:  []string{"M18S", "M19S"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			var stopTimeUpdates []*gtfsrt.TripUpdate_StopTimeUpdate
			for _, stopID := range tc.InputStopIDs {
				stopID := stopID
				stopTimeUpdates = append(stopTimeUpdates, &gtfsrt.TripUpdate_StopTimeUpdate{
					StopId: &stopID,
				})
			}
			entities := []*gtfsrt.FeedEntity{
				{
					Id: ptr("1"),
					TripUpdate: &gtfsrt.TripUpdate{
						Trip: &gtfsrt.TripDescriptor{
							TripId:  ptr(tripID1),
							RouteId: ptr(tc.RouteID),
						},
						StopTimeUpdate: stopTimeUpdates,
					},
				},
			}
			result := testutil.MustParse(t, nil, entities, &gtfs.ParseRealtimeOptions{
				Extension: nycttrips.Extension(nycttrips.ExtensionOpts{
					PreserveMTrainPlatformsInBushwick: tc.FlagValue,
				}),
			})
			if len(result.Trips) != 1 {
				t.Fatalf("len(result.Trips)=%d, wanted 1", len(result.Trips))
			}
			var gotStopIDs []string
			for _, stopTimeUpdate := range result.Trips[0].StopTimeUpdates {
				gotStopIDs = append(gotStopIDs, *stopTimeUpdate.StopID)
			}
			if !reflect.DeepEqual(gotStopIDs, tc.WantStopIDs) {
				t.Errorf("stopIDs got = %v, want = %v", gotStopIDs, tc.WantStopIDs)
			}
		})
	}
}

func ptr(s string) *string {
	return &s
}
