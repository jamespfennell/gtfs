// Package nycttrips contains logic for the New York City Transit GTFS realtime extenstions
package nycttrips

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/jamespfennell/gtfs/extensions"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
	"google.golang.org/protobuf/proto"
)

var TripIDRegex *regexp.Regexp = regexp.MustCompile(`^([0-9]{6})_([[:alnum:]]{1,2})..([SN])([[:alnum:]]*)$`)

// ExtensionOpts contains the options for the NYCT trips extension.
type ExtensionOpts struct {
	// Filter out trips which are scheduled to run in the past but have no assigned trip and haven't started.
	FilterStaleUnassignedTrips bool `yaml:"filterStaleUnassignedTrips"`
}

// Extension returns the NYCT trips extension
func Extension(opts ExtensionOpts) extensions.Extension {
	return extension{
		opts: opts,
	}
}

type extension struct {
	opts ExtensionOpts

	extensions.NoExtensionImpl
}

func (e extension) UpdateTrip(trip *gtfsrt.TripUpdate, feedCreatedAt uint64) extensions.UpdateTripResult {
	// TODO: ensure that if isAssigned is true then the vehicle is populated, otherwise populate it somehow
	isAssigned := e.updateTripOrVehicle(trip)
	shouldSkip := proto.HasExtension(trip.GetTrip(), gtfsrt.E_NyctTripDescriptor) &&
		e.opts.FilterStaleUnassignedTrips && isStaleUnassignedTrip(isAssigned, trip.StopTimeUpdate, feedCreatedAt)
	return extensions.UpdateTripResult{
		ShouldSkip:     shouldSkip,
		NyctIsAssigned: isAssigned,
	}
}

func (e extension) UpdateVehicle(vehicle *gtfsrt.VehiclePosition) {
	e.updateTripOrVehicle(vehicle)
}

type tripOrVehicle interface {
	GetTrip() *gtfsrt.TripDescriptor
}

func (e extension) updateTripOrVehicle(entity tripOrVehicle) bool {
	tripDesc := entity.GetTrip()
	if !proto.HasExtension(tripDesc, gtfsrt.E_NyctTripDescriptor) {
		return false
	}
	extendedEvent := proto.GetExtension(tripDesc, gtfsrt.E_NyctTripDescriptor)
	nyctTripDesc, _ := extendedEvent.(*gtfsrt.NyctTripDescriptor)

	if nyctTripDesc.GetIsAssigned() {
		// TODO: is it possible the train ID is not set?
		// TODO: is it possible there is already a vehicle descriptor here
		id := nyctTripDesc.GetTrainId()
		setVehicleDescriptor(entity, &gtfsrt.VehicleDescriptor{
			Id: &id,
		})
	}

	directionIDNorth := uint32(0)
	directionIDSouth := uint32(1)
	if nyctTripDesc.GetDirection() == gtfsrt.NyctTripDescriptor_NORTH {
		tripDesc.DirectionId = &directionIDNorth
	} else {
		tripDesc.DirectionId = &directionIDSouth
	}

	nyctTripIDMatch := TripIDRegex.FindStringSubmatch(tripDesc.GetTripId())
	if nyctTripIDMatch != nil {
		// We ignore the error as the regex guarantees it works
		hundrethsOfMins, _ := strconv.Atoi(nyctTripIDMatch[1])
		secondsAfterMidnight := (hundrethsOfMins * 6) / 10
		minutesAfterMidnight := secondsAfterMidnight / 60
		startTime := fmt.Sprintf("%02d:%02d:%02d", minutesAfterMidnight/60, minutesAfterMidnight%60, secondsAfterMidnight%60)
		tripDesc.StartTime = &startTime
	}
	return nyctTripDesc.GetIsAssigned()
}

func setVehicleDescriptor(entity tripOrVehicle, vehicleDesc *gtfsrt.VehicleDescriptor) {
	switch t := entity.(type) {
	case *gtfsrt.TripUpdate:
		t.Vehicle = vehicleDesc
	case *gtfsrt.VehiclePosition:
		t.Vehicle = vehicleDesc
	}
}

func isStaleUnassignedTrip(isAssigned bool, stopTimes []*gtfsrt.TripUpdate_StopTimeUpdate, feedCreatedAt uint64) bool {
	if isAssigned {
		return false
	}
	if len(stopTimes) == 0 {
		return true
	}
	stopTime := stopTimes[0]
	firstTime := stopTime.GetDeparture().GetTime()
	if firstTime == 0 {
		firstTime = stopTime.GetArrival().GetTime()
	}
	if firstTime == 0 {
		return true
	}
	return firstTime < int64(feedCreatedAt)
}

func (e extension) GetTrack(stopTimeUpdate *gtfsrt.TripUpdate_StopTimeUpdate) *string {
	if !proto.HasExtension(stopTimeUpdate, gtfsrt.E_NyctStopTimeUpdate) {
		return nil
	}
	extendedEvent := proto.GetExtension(stopTimeUpdate, gtfsrt.E_NyctStopTimeUpdate)
	nyctStopTimeUpdate, _ := extendedEvent.(*gtfsrt.NyctStopTimeUpdate)
	if nyctStopTimeUpdate.ActualTrack != nil {
		return nyctStopTimeUpdate.ActualTrack
	}
	return nyctStopTimeUpdate.ScheduledTrack
}
