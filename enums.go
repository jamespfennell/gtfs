package gtfs

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

func parseRouteType(s string) RouteType {
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
	}
	return "UNKNOWN"
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
		fallthrough
	default:
		return "NOT_ALLOWED"
	}
}
