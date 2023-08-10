package nyctbustrips_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jamespfennell/gtfs"
	"github.com/jamespfennell/gtfs/extensions/nyctbustrips"
	"github.com/jamespfennell/gtfs/internal/testutil"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
)

const tripID1 = "tripID1"
const vehicleID1 = "vehicleID1"
const vehicleID2 = "vehicle ID 2"

func TestUpdatesTripId(t *testing.T) {
	testCases := []struct {
		VehicleId   string
		DirectionId *uint32
	}{
		{
			VehicleId:   vehicleID1,
			DirectionId: uint32Ptr(1),
		},
		{
			VehicleId: vehicleID2,
		},
	}
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			entities := []*gtfsrt.FeedEntity{
				{
					Id: ptr("1"),
					TripUpdate: &gtfsrt.TripUpdate{
						Trip: &gtfsrt.TripDescriptor{
							TripId:      ptr(tripID1),
							DirectionId: testCase.DirectionId,
						},
						Vehicle: &gtfsrt.VehicleDescriptor{
							Id: ptr(testCase.VehicleId),
						},
					},
				},
			}

			result := testutil.MustParse(t, nil, entities, &gtfs.ParseRealtimeOptions{
				Extension: nyctbustrips.Extension(),
			})

			var expectedTripId string = ""
			vehicleId := testCase.VehicleId

			var vehicle *gtfs.Vehicle = nil
			if vehicleId != "" {
				vehicle = &gtfs.Vehicle{
					ID: &gtfs.VehicleID{
						ID: testCase.VehicleId,
					},
					IsEntityInMessage: false,
				}
			}

			var directionId gtfs.DirectionID = gtfs.DirectionID_Unspecified
			if testCase.DirectionId != nil {
				if *testCase.DirectionId == 0 {
					directionId = gtfs.DirectionID_False
				} else {
					directionId = gtfs.DirectionID_True
				}
			}

			expectedTripId = strings.Replace(fmt.Sprintf("%s_%s_%d", tripID1, vehicleId, directionId), " ", "_", -1)
			expected := []gtfs.Trip{
				{
					ID: gtfs.TripID{
						ID:          expectedTripId,
						DirectionID: directionId,
					},
					Vehicle:           vehicle,
					IsEntityInMessage: true,
				},
			}

			if vehicle != nil {
				expected[0].Vehicle.Trip = &expected[0]
			}

			if !reflect.DeepEqual(result.Trips, expected) {
				t.Errorf("actual:\n%+v\n!= expected:\n%+v", result.Trips, expected)
			}
		})
	}
}

func ptr(s string) *string {
	return &s
}

func uint32Ptr(n uint32) *uint32 {
	return &n
}
