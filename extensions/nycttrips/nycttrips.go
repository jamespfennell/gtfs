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

	// The raw MTA data has a bug in which the M train platforms are reported incorrectly for stations
	// in Williamsburg and Bushwick that the M shares with the J train. In the raw data, M trains going towards
	// the Williamsburg bridge stop at M11N, but J trains going towards the bridge stop at M11S. By default
	// this extension fixes these platforms for the M train, so M11N becomes M11S. This fix can be disabled
	// by setting this option to true.
	PreserveMTrainPlatformsInBushwick bool `yaml:"preserveMTrainPlatformsInBushwick"`
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
	if !e.opts.PreserveMTrainPlatformsInBushwick {
		fixMTrainPlatformsInBushwick(trip)
	}
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

func fixMTrainPlatformsInBushwick(trip *gtfsrt.TripUpdate) {
	if trip.GetTrip().GetRouteId() != "M" {
		return
	}
	buggyStationIDs := map[string]bool{
		"M11": true,
		"M12": true,
		"M13": true,
		"M14": true,
		"M16": true,
		"M18": true,
	}
	for _, stopTimeUpdate := range trip.GetStopTimeUpdate() {
		if stopTimeUpdate == nil {
			continue
		}
		stopID := stopTimeUpdate.GetStopId()
		if len(stopID) != 4 {
			continue
		}
		if !buggyStationIDs[stopID[:3]] {
			continue
		}
		newDirection := 'N'
		if stopID[3] == 'N' {
			newDirection = 'S'
		}
		newStopID := stopID[:3] + string(newDirection)
		stopTimeUpdate.StopId = &newStopID
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
