package extensions

import (
	gtfsrt "github.com/jamespfennell/gtfs/proto"
)

type Extension interface {
	// TODO PreprocessTrip etc
	// TODO remove isAssigned
	UpdateTrip(trip *gtfsrt.TripUpdate, feedCreatedAt uint64) UpdateTripResult

	UpdateVehicle(vehicle *gtfsrt.VehiclePosition)

	UpdateAlert(ID *string, alert *gtfsrt.Alert) bool

	GetTrack(stopTimeUpdate *gtfsrt.TripUpdate_StopTimeUpdate) *string
}

type UpdateTripResult struct {
	// Whether this trip should be skipped.
	ShouldSkip bool
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

func (n NoExtensionImpl) UpdateAlert(ID *string, alert *gtfsrt.Alert) bool {
	return false
}

func (n NoExtensionImpl) GetTrack(stopTimeUpdate *gtfsrt.TripUpdate_StopTimeUpdate) *string {
	return nil
}
