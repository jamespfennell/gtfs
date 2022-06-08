package extensions

import (
	gtfsrt "github.com/jamespfennell/gtfs/proto"
)

type Extension interface {
	UpdateTrip(trip *gtfsrt.TripUpdate, feedCreatedAt uint64) UpdateTripResult

	UpdateVehicle(vehicle *gtfsrt.VehiclePosition)

	UpdateAlert(alert *gtfsrt.Alert)

	GetTrack(stopTimeUpdate *gtfsrt.TripUpdate_StopTimeUpdate) *string
}

type UpdateTripResult struct {
	// Whether this trip should be skipped.
	ShouldSkip bool

	// Value of the NyctIsAssigned field if the entity is a trip. This field is ignored for vehicles.
	NyctIsAssigned bool
}

func NoExtension() Extension {
	return NoExtensionImpl{}
}

type NoExtensionImpl struct {
}

func (n NoExtensionImpl) UpdateTrip(trip *gtfsrt.TripUpdate, feedCreatedAt uint64) UpdateTripResult {
	return UpdateTripResult{}
}

func (n NoExtensionImpl) UpdateVehicle(vehicle *gtfsrt.VehiclePosition) {
}

func (n NoExtensionImpl) UpdateAlert(alert *gtfsrt.Alert) {
}

func (n NoExtensionImpl) GetTrack(stopTimeUpdate *gtfsrt.TripUpdate_StopTimeUpdate) *string {
	return nil
}
