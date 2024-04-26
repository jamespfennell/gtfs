package gtfs

import "strconv"

// BikesAllowed describes whether bikes are allowed on a scheduled trip.
//
// This is a Go representation of the enum described in the `bikes_allowed` field of `stops.txt`.
type BikesAllowed int32

const (
	BikesAllowed_NotSpecified BikesAllowed = 0
	BikesAllowed_Allowed      BikesAllowed = 1
	BikesAllowed_NotAllowed   BikesAllowed = 2
)

func (b BikesAllowed) String() string {
	switch b {
	case BikesAllowed_NotSpecified:
		return "NOT_SPECIFIED"
	case BikesAllowed_Allowed:
		return "ALLOWED"
	case BikesAllowed_NotAllowed:
		return "NOT_ALLOWED"
	default:
		return "UNKNOWN"
	}
}

func parseBikesAllowed(s string) BikesAllowed {
	switch s {
	case "1":
		return BikesAllowed_Allowed
	case "2":
		return BikesAllowed_NotAllowed
	default:
		return BikesAllowed_NotSpecified
	}
}

// DirectionID is a mechanism for distinguishing between trips going in the opposite direction.
type DirectionID uint8

const (
	DirectionID_Unspecified DirectionID = 0
	DirectionID_True        DirectionID = 1
	DirectionID_False       DirectionID = 2
)

func parseDirectionID_GTFSStatic(s string) DirectionID {
	switch s {
	case "0":
		return DirectionID_False
	case "1":
		return DirectionID_True
	default:
		return DirectionID_Unspecified
	}
}

func parseDirectionID_GTFSRealtime(raw *uint32) DirectionID {
	if raw == nil {
		return DirectionID_Unspecified
	}
	if *raw == 0 {
		return DirectionID_False
	}
	return DirectionID_True
}

func (d DirectionID) String() string {
	switch d {
	case DirectionID_True:
		return "TRUE"
	case DirectionID_False:
		return "FALSE"
	default:
		return "UNSPECIFIED"
	}
}

// ExactTimes describes the type of service for a trip.
//
// This is a Go representation of the enum described in the `exact_times` field of `frequencies.txt`.
type ExactTimes int32

const (
	FrequencyBased ExactTimes = 0
	ScheduleBased  ExactTimes = 1
)

func parseExactTimes(s string) ExactTimes {
	switch s {
	case "0":
		return FrequencyBased
	case "1":
		return ScheduleBased
	default:
		return FrequencyBased
	}
}

func (t ExactTimes) String() string {
	switch t {
	case ScheduleBased:
		return "SCHEDULE_BASED"
	case FrequencyBased:
		fallthrough
	default:
		return "FREQUENCY_BASED"
	}
}

// PickupDropOffPolicy describes the pickup or drop-off policy for a route or scheduled trip.
//
// This is a Go representation of the enum described in the `continuous_pickup` field of `routes.txt`,
// and `pickup_type` field of `stop_times.txt`, and similar fields.
type PickupDropOffPolicy int32

const (
	// Pickup or drop off happens by default.
	PickupDropOffPolicy_Yes PickupDropOffPolicy = 0
	// No pickup or drop off is possible.
	PickupDropOffPolicy_No PickupDropOffPolicy = 1
	// Must phone an agency to arrange pickup or drop off.
	PickupDropOffPolicy_PhoneAgency PickupDropOffPolicy = 2
	// Must coordinate with a driver to arrange pickup or drop off.
	PickupDropOffPolicy_CoordinateWithDriver PickupDropOffPolicy = 3
)

func parsePickupDropOffPolicy(s string) PickupDropOffPolicy {
	switch s {
	case "0":
		return PickupDropOffPolicy_Yes
	case "2":
		return PickupDropOffPolicy_PhoneAgency
	case "3":
		return PickupDropOffPolicy_CoordinateWithDriver
	default:
		return PickupDropOffPolicy_No
	}
}

func (t PickupDropOffPolicy) String() string {
	switch t {
	case PickupDropOffPolicy_Yes:
		return "ALLOWED"
	case PickupDropOffPolicy_PhoneAgency:
		return "PHONE_AGENCY"
	case PickupDropOffPolicy_CoordinateWithDriver:
		return "COORDINATE_WITH_DRIVER"
	case PickupDropOffPolicy_No:
		return "NOT_ALLOWED"
	default:
		return "UNKNOWN"
	}
}

// RouteType describes the type of a route.
//
// This is a Go representation of the enum described in the `route_type` field of `routes.txt`.
type RouteType int32

const (
	RouteType_Tram       RouteType = 0
	RouteType_Subway     RouteType = 1
	RouteType_Rail       RouteType = 2
	RouteType_Bus        RouteType = 3
	RouteType_Ferry      RouteType = 4
	RouteType_CableTram  RouteType = 5
	RouteType_AerialLift RouteType = 6
	RouteType_Funicular  RouteType = 7
	RouteType_TrolleyBus RouteType = 11
	RouteType_Monorail   RouteType = 12

	RouteType_Unknown RouteType = 10000
)

func parseRouteType_GTFSStatic(s string) RouteType {
	switch s {
	case "0":
		return RouteType_Tram
	case "1":
		return RouteType_Subway
	case "2":
		return RouteType_Rail
	case "3":
		return RouteType_Bus
	case "4":
		return RouteType_Ferry
	case "5":
		return RouteType_CableTram
	case "6":
		return RouteType_AerialLift
	case "7":
		return RouteType_Funicular
	case "11":
		return RouteType_TrolleyBus
	case "12":
		return RouteType_Monorail
	default:
		return RouteType_Unknown
	}
}

func parseRouteType_GTFSRealtime(raw *int32) RouteType {
	if raw == nil {
		return RouteType_Unknown
	}
	return parseRouteType_GTFSStatic(strconv.FormatInt(int64(*raw), 10))
}

func (t RouteType) String() string {
	switch t {
	case RouteType_Tram:
		return "TRAM"
	case RouteType_Subway:
		return "SUBWAY"
	case RouteType_Rail:
		return "RAIL"
	case RouteType_Bus:
		return "BUS"
	case RouteType_Ferry:
		return "FERRY"
	case RouteType_CableTram:
		return "CABLE_TRAM"
	case RouteType_AerialLift:
		return "AERIAL_LIFT"
	case RouteType_Funicular:
		return "FUNICULAR"
	case RouteType_TrolleyBus:
		return "TROLLEY_BUS"
	case RouteType_Monorail:
		return "MONORAIL"
	default:
		return "UNKNOWN"
	}
}

// StopType describes the type of a stop.
//
// This is a Go representation of the enum described in the `location_type` field of `stops.txt`.
type StopType int32

const (
	StopType_Stop           StopType = 0
	StopType_Station        StopType = 1
	StopType_EntranceOrExit StopType = 2
	StopType_GenericNode    StopType = 3
	StopType_BoardingArea   StopType = 4
	StopType_Platform       StopType = 5
)

func parseStopType(s string, hasParentStop bool) StopType {
	switch s {
	case "1":
		return StopType_Station
	case "2":
		return StopType_EntranceOrExit
	case "3":
		return StopType_GenericNode
	case "4":
		return StopType_BoardingArea
	default:
		if hasParentStop {
			return StopType_Platform
		} else {
			return StopType_Stop
		}
	}
}

func (t StopType) String() string {
	switch t {
	case StopType_Stop:
		return "STOP"
	case StopType_Platform:
		return "PLATFORM"
	case StopType_Station:
		return "STATION"
	case StopType_EntranceOrExit:
		return "ENTRANCE_OR_EXIT"
	case StopType_GenericNode:
		return "GENERIC_NODE"
	case StopType_BoardingArea:
		return "BOARDING_AREA"
	default:
		return "UNKNOWN"
	}
}

// StopType describes the type of a transfer.
//
// This is a Go representation of the enum described in the `transfer_type` field of `transfers.txt`.
type TransferType int32

const (
	TransferType_Recommended  TransferType = 0
	TransferType_Timed        TransferType = 1
	TransferType_RequiresTime TransferType = 2
	TransferType_NotPossible  TransferType = 3
)

func parseTransferType(s string) TransferType {
	switch s {
	case "1":
		return TransferType_Timed
	case "2":
		return TransferType_RequiresTime
	case "3":
		return TransferType_NotPossible
	default:
		return TransferType_Recommended
	}
}

func (t TransferType) String() string {
	switch t {
	case TransferType_Recommended:
		return "RECOMMENDED"
	case TransferType_Timed:
		return "TIMED"
	case TransferType_RequiresTime:
		return "REQUIRES_TIME"
	case TransferType_NotPossible:
		return "NOT_POSSIBLE"
	default:
		return "UNKNOWN"
	}
}

// WheelchairBoarding describes whether wheelchair boarding is available at a stop.
//
// This is a Go representation of the enum described in the `wheelchair_boarding` field of `stops.txt`
// and `wheelchair_accessible` field of `trips.txt`.
type WheelchairBoarding int32

const (
	WheelchairBoarding_NotSpecified WheelchairBoarding = 0
	WheelchairBoarding_Possible     WheelchairBoarding = 1
	WheelchairBoarding_NotPossible  WheelchairBoarding = 2
)

func parseWheelchairBoarding(s string) WheelchairBoarding {
	switch s {
	case "1":
		return WheelchairBoarding_Possible
	case "2":
		return WheelchairBoarding_NotPossible
	default:
		return WheelchairBoarding_NotSpecified
	}
}

func (w WheelchairBoarding) String() string {
	switch w {
	case WheelchairBoarding_NotSpecified:
		return "NOT_SPECIFIED"
	case WheelchairBoarding_Possible:
		return "POSSIBLE"
	case WheelchairBoarding_NotPossible:
		return "NOT_POSSIBLE"
	default:
		return "UNKNOWN"
	}
}
