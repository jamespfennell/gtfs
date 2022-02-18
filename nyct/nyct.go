// Package nyct contains logic for the New York City Transit GTFS realtime extenstions
package nyct

import (
	"fmt"
	"regexp"
	"strconv"

	gtfsrt "github.com/jamespfennell/gtfs/proto"
	"google.golang.org/protobuf/proto"
)

var TripIDRegex *regexp.Regexp = regexp.MustCompile(`^([0-9]{6})_([[:alnum:]]{1,2})..([SN])([[:alnum:]]*)$`)

type HasTripDescriptor interface {
	GetTrip() *gtfsrt.TripDescriptor
}

// UpdateDescriptors populates fields in the GTFS trip and vehicle descriptors
// using data in the NYCT trip descriptors extension.
func UpdateDescriptors(entity HasTripDescriptor) bool {
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

func setVehicleDescriptor(entity HasTripDescriptor, vehicleDesc *gtfsrt.VehicleDescriptor) {
	switch t := entity.(type) {
	case *gtfsrt.TripUpdate:
		t.Vehicle = vehicleDesc
	case *gtfsrt.VehiclePosition:
		t.Vehicle = vehicleDesc
	}
}

func GetTrack(stopTimeUpdate *gtfsrt.TripUpdate_StopTimeUpdate) *string {
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
