package nyctbustrips

import (
	"fmt"
	"strings"

	"github.com/jamespfennell/gtfs/extensions"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
)

// Extension returns the NYCT bus trips extension
func Extension() extensions.Extension {
	return extension{}
}

type extension struct {
	extensions.NoExtensionImpl
}

func (e extension) UpdateTrip(trip *gtfsrt.TripUpdate, feedCreatedAt uint64) extensions.UpdateTripResult {
	tripId := trip.GetTrip().GetTripId()
	vehicleId := trip.GetVehicle().GetId()
	direction := trip.GetTrip().GetDirectionId()
	compositeTripId := strings.Replace(fmt.Sprintf("%s_%s_%d", tripId, vehicleId, direction), " ", "_", -1)

	trip.Trip.TripId = &compositeTripId

	return extensions.UpdateTripResult{}
}
