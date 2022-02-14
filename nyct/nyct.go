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

type HasDescriptors interface {
	GetTrip() *gtfsrt.TripDescriptor
	GetVehicle() *gtfsrt.VehicleDescriptor
}

func UpdateDescriptors(entity HasDescriptors) bool {
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

func setVehicleDescriptor(entity HasDescriptors, vehicleDesc *gtfsrt.VehicleDescriptor) {
	switch t := entity.(type) {
	case *gtfsrt.TripUpdate:
		t.Vehicle = vehicleDesc
	case *gtfsrt.VehiclePosition:
		t.Vehicle = vehicleDesc
	}
}
