package gtfs_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/jamespfennell/gtfs"
	"github.com/jamespfennell/gtfs/internal/testutil"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
)

const tripID1 = "tripID1"
const vehicleID1 = "vehicleID1"

var time1 time.Time = time.Unix(2<<28, 0).UTC()

func TestSoloTrip(t *testing.T) {
	timestamp := uint64(time1.Unix())
	header := &gtfsrt.FeedHeader{
		GtfsRealtimeVersion: ptr("2.0"),
		Timestamp:           &timestamp,
	}
	entities := []*gtfsrt.FeedEntity{
		{
			Id: ptr("1"),
			TripUpdate: &gtfsrt.TripUpdate{
				Trip: &gtfsrt.TripDescriptor{
					TripId: ptr(tripID1),
				},
				Vehicle: &gtfsrt.VehicleDescriptor{
					Id: ptr(vehicleID1),
				},
			},
		},
	}

	trip := gtfs.Trip{
		ID: gtfs.TripID{
			ID:          tripID1,
			DirectionID: gtfs.DirectionIDUnspecified,
		},
		IsEntityInMessage: true,
	}
	vehicle := gtfs.Vehicle{
		ID: &gtfs.VehicleID{
			ID: vehicleID1,
		},
		IsEntityInMessage: false,
	}
	trip.Vehicle = &vehicle
	vehicle.Trip = &trip
	expectedResult := &gtfs.Realtime{
		CreatedAt: time1,
		Trips:     []gtfs.Trip{trip},
		Vehicles:  []gtfs.Vehicle{vehicle},
	}

	result := testutil.MustParse(t, header, entities, &gtfs.ParseRealtimeOptions{})

	if !reflect.DeepEqual(result, expectedResult) {
		t.Errorf("actual:\n%+v\n!= expected:\n%+v", result, expectedResult)
	}
}

func ptr(s string) *string {
	return &s
}
