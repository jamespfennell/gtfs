package extensions

import (
	gtfsrt "github.com/jamespfennell/gtfs/proto"
)

type Extension interface {
	UpdateTripOrVehicle(entity TripOrVehicle, feedCreatedAt uint64) UpdateTripOrVehicleResult

	GetTrack(stopTimeUpdate *gtfsrt.TripUpdate_StopTimeUpdate) *string
}

type UpdateTripOrVehicleResult struct {
	// Whether this entitity should be skipped.
	ShouldSkip bool

	// Value of the NyctIsAssigned field if the entity is a trip. This field is ignored for vehicles.
	NyctIsAssigned bool
}

// TripOrVehicle is either a TripUpdate or VehiclePosition proto message.
//
// The original type can be extracted using a type switch.
type TripOrVehicle interface {
	GetTrip() *gtfsrt.TripDescriptor
}

func NoExtension() Extension {
	return NoExtensionImpl{}
}

type NoExtensionImpl struct {
}

func (n NoExtensionImpl) UpdateTripOrVehicle(entity TripOrVehicle, feedCreatedAt uint64) UpdateTripOrVehicleResult {
	return UpdateTripOrVehicleResult{}
}

func (n NoExtensionImpl) GetTrack(stopTimeUpdate *gtfsrt.TripUpdate_StopTimeUpdate) *string {
	return nil
}
