package gtfs

import (
	"reflect"
	"testing"
	"time"

	gtfsrt "github.com/jamespfennell/gtfs/proto"
	"google.golang.org/protobuf/proto"
)

const tripID1 = "tripID1"
const vehicleID1 = "vehicleID1"

var time1 time.Time = time.Unix(2<<28, 0).UTC()

func TestSoloTrip(t *testing.T) {

	timestamp := uint64(time1.Unix())
	message := gtfsrt.FeedMessage{
		Header: &gtfsrt.FeedHeader{
			GtfsRealtimeVersion: ptr("2.0"),
			Timestamp:           &timestamp,
		},
		Entity: []*gtfsrt.FeedEntity{
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
		},
	}
	b, err := proto.Marshal(&message)
	if err != nil {
		t.Fatalf("Failed to marshal message: %s", err)
	}

	trip := Trip{
		ID: TripID{
			ID:          tripID1,
			DirectionID: DirectionIDUnspecified,
		},
		IsEntityInMessage: true,
	}
	vehicle := Vehicle{
		ID: &VehicleID{
			ID: vehicleID1,
		},
		IsEntityInMessage: false,
	}
	trip.Vehicle = &vehicle
	vehicle.Trip = &trip
	expectedResult := &Realtime{
		CreatedAt: time1,
		Trips:     []Trip{trip},
		Vehicles:  []Vehicle{vehicle},
	}

	result, err := ParseRealtime(b, &ParseRealtimeOptions{})
	if err != nil {
		t.Errorf("unexpected error in ParseRealtime: %s", err)
	}
	if !reflect.DeepEqual(result, expectedResult) {
		t.Errorf("actual:\n%+v\n!= expected:\n%+v", result, expectedResult)
	}
}
