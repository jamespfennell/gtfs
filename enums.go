package gtfs

import (
	"fmt"
	"strconv"
	"strings"
)

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

	RouteType_RailwayService              RouteType = 100
	RouteType_HighSpeedRailService        RouteType = 101
	RouteType_LongDistanceRailService     RouteType = 102
	RouteType_InterRegionalRailService    RouteType = 103
	RouteType_CarTransportRailService     RouteType = 104
	RouteType_SleeperRailService          RouteType = 105
	RouteType_RegionalRailService         RouteType = 106
	RouteType_TouristRailwayService       RouteType = 107
	RouteType_RailShuttle                 RouteType = 108
	RouteType_SuburbanRailway             RouteType = 109
	RouteType_ReplacementRailService      RouteType = 110
	RouteType_SpecialRailService          RouteType = 111
	RouteType_LorryTransportRailService   RouteType = 112
	RouteType_AllRailServices             RouteType = 113
	RouteType_CrossCountryRailService     RouteType = 114
	RouteType_VehicleTransportRailService RouteType = 115
	RouteType_RackAndPinionRailway        RouteType = 116
	RouteType_AdditionalRailService       RouteType = 117

	RouteType_CoachService         RouteType = 200
	RouteType_InternationalCoach   RouteType = 201
	RouteType_NationalCoach        RouteType = 202
	RouteType_ShuttleCoach         RouteType = 203
	RouteType_RegionalCoach        RouteType = 204
	RouteType_SpecialCoach         RouteType = 205
	RouteType_TouristCoach         RouteType = 206
	RouteType_CommuterCoach        RouteType = 207
	RouteType_AllCoachServices     RouteType = 208
	RouteType_SuburbanCoachService RouteType = 209

	RouteType_UrbanRailwayService     RouteType = 400
	RouteType_MetroService            RouteType = 401
	RouteType_UndergroundService      RouteType = 402
	RouteType_UrbanRailway            RouteType = 403
	RouteType_AllUrbanRailwayServices RouteType = 404
	RouteType_UrbanMonorail           RouteType = 405

	RouteType_BusService                    RouteType = 700
	RouteType_RegionalBus                   RouteType = 701
	RouteType_ExpressBus                    RouteType = 702
	RouteType_StoppingBus                   RouteType = 703
	RouteType_LocalBus                      RouteType = 704
	RouteType_NightBus                      RouteType = 705
	RouteType_PostBus                       RouteType = 706
	RouteType_SpecialNeedsBus               RouteType = 707
	RouteType_MobilityBus                   RouteType = 708
	RouteType_MobilityBusRegisteredDisabled RouteType = 709
	RouteType_SightseeingBus                RouteType = 710
	RouteType_ShuttleBus                    RouteType = 711
	RouteType_SchoolBus                     RouteType = 712
	RouteType_SchoolPublicServiceBus        RouteType = 713
	RouteType_RailReplacementBus            RouteType = 714
	RouteType_DemandResponseBus             RouteType = 715
	RouteType_AllBusServices                RouteType = 716
	RouteType_ShareTaxiBus                  RouteType = 717

	RouteType_TrolleybusService RouteType = 800

	RouteType_TramService     RouteType = 900
	RouteType_CityTram        RouteType = 901
	RouteType_LocalTram       RouteType = 902
	RouteType_RegionalTram    RouteType = 903
	RouteType_SightseeingTram RouteType = 904
	RouteType_ShuttleTram     RouteType = 905
	RouteType_AllTramServices RouteType = 906
	RouteType_CrossborderTram RouteType = 907

	RouteType_WaterTransportService       RouteType = 1000
	RouteType_InternationalCarFerry       RouteType = 1001
	RouteType_NationalCarFerry            RouteType = 1002
	RouteType_RegionalCarFerry            RouteType = 1003
	RouteType_LocalCarFerry               RouteType = 1004
	RouteType_InternationalPassengerFerry RouteType = 1005
	RouteType_NationalPassengerFerry      RouteType = 1006
	RouteType_RegionalPassengerFerry      RouteType = 1007
	RouteType_LocalPassengerFerry         RouteType = 1008
	RouteType_PostBoat                    RouteType = 1009
	RouteType_TrainFerry                  RouteType = 1010
	RouteType_RoadLinkFerry               RouteType = 1011
	RouteType_AirportLinkFerry            RouteType = 1012
	RouteType_CarHighSpeedFerry           RouteType = 1013
	RouteType_PassengerHighSpeedFerry     RouteType = 1014
	RouteType_SightseeingBoat             RouteType = 1015
	RouteType_SchoolBoat                  RouteType = 1016
	RouteType_CableDrawnBoat              RouteType = 1017
	RouteType_RiverBus                    RouteType = 1018
	RouteType_ScheduledFerry              RouteType = 1019
	RouteType_ShuttleFerry                RouteType = 1020
	RouteType_AllWaterTransportServices   RouteType = 1021

	RouteType_AirService                  RouteType = 1100
	RouteType_InternationalAirService     RouteType = 1101
	RouteType_DomesticAirService          RouteType = 1102
	RouteType_IntercontinentalAirService  RouteType = 1103
	RouteType_DomesticScheduledAirService RouteType = 1104
	RouteType_ShuttleAirService           RouteType = 1105
	RouteType_IntercontinentalCharterAir  RouteType = 1106
	RouteType_InternationalCharterAir     RouteType = 1107
	RouteType_RoundTripCharterAir         RouteType = 1108
	RouteType_SightseeingAirService       RouteType = 1109
	RouteType_HelicopterAirService        RouteType = 1110
	RouteType_DomesticCharterAirService   RouteType = 1111
	RouteType_AllAirServices              RouteType = 1112

	RouteType_FerryService RouteType = 1200

	RouteType_AerialLiftService    RouteType = 1300
	RouteType_TelecabinService     RouteType = 1301
	RouteType_CableCarService      RouteType = 1302
	RouteType_ElevatorService      RouteType = 1303
	RouteType_ChairLiftService     RouteType = 1304
	RouteType_DragLiftService      RouteType = 1305
	RouteType_SmallTelecabin       RouteType = 1306
	RouteType_AllTelecabinServices RouteType = 1307

	RouteType_FunicularService    RouteType = 1400
	RouteType_Funicular_1         RouteType = 1401
	RouteType_AllFunicularService RouteType = 1402

	RouteType_TaxiService        RouteType = 1500
	RouteType_CommunalTaxi       RouteType = 1501
	RouteType_WaterTaxi          RouteType = 1502
	RouteType_RailTaxi           RouteType = 1503
	RouteType_BikeTaxi           RouteType = 1504
	RouteType_LicensedTaxi       RouteType = 1505
	RouteType_PrivateHireVehicle RouteType = 1506
	RouteType_AllTaxiServices    RouteType = 1507

	RouteType_MiscellaneousService RouteType = 1700
	RouteType_CableCarMisc         RouteType = 1701
	RouteType_HorseDrawnCarriage   RouteType = 1702

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

	case "100":
		return RouteType_RailwayService
	case "101":
		return RouteType_HighSpeedRailService
	case "102":
		return RouteType_LongDistanceRailService
	case "103":
		return RouteType_InterRegionalRailService
	case "104":
		return RouteType_CarTransportRailService
	case "105":
		return RouteType_SleeperRailService
	case "106":
		return RouteType_RegionalRailService
	case "107":
		return RouteType_TouristRailwayService
	case "108":
		return RouteType_RailShuttle
	case "109":
		return RouteType_SuburbanRailway
	case "110":
		return RouteType_ReplacementRailService
	case "111":
		return RouteType_SpecialRailService
	case "112":
		return RouteType_LorryTransportRailService
	case "113":
		return RouteType_AllRailServices
	case "114":
		return RouteType_CrossCountryRailService
	case "115":
		return RouteType_VehicleTransportRailService
	case "116":
		return RouteType_RackAndPinionRailway
	case "117":
		return RouteType_AdditionalRailService

	case "200":
		return RouteType_CoachService
	case "201":
		return RouteType_InternationalCoach
	case "202":
		return RouteType_NationalCoach
	case "203":
		return RouteType_ShuttleCoach
	case "204":
		return RouteType_RegionalCoach
	case "205":
		return RouteType_SpecialCoach
	case "206":
		return RouteType_TouristCoach
	case "207":
		return RouteType_CommuterCoach
	case "208":
		return RouteType_AllCoachServices
	case "209":
		return RouteType_SuburbanCoachService

	case "400":
		return RouteType_UrbanRailwayService
	case "401":
		return RouteType_MetroService
	case "402":
		return RouteType_UndergroundService
	case "403":
		return RouteType_UrbanRailway
	case "404":
		return RouteType_AllUrbanRailwayServices
	case "405":
		return RouteType_UrbanMonorail

	case "700":
		return RouteType_BusService
	case "701":
		return RouteType_RegionalBus
	case "702":
		return RouteType_ExpressBus
	case "703":
		return RouteType_StoppingBus
	case "704":
		return RouteType_LocalBus
	case "705":
		return RouteType_NightBus
	case "706":
		return RouteType_PostBus
	case "707":
		return RouteType_SpecialNeedsBus
	case "708":
		return RouteType_MobilityBus
	case "709":
		return RouteType_MobilityBusRegisteredDisabled
	case "710":
		return RouteType_SightseeingBus
	case "711":
		return RouteType_ShuttleBus
	case "712":
		return RouteType_SchoolBus
	case "713":
		return RouteType_SchoolPublicServiceBus
	case "714":
		return RouteType_RailReplacementBus
	case "715":
		return RouteType_DemandResponseBus
	case "716":
		return RouteType_AllBusServices
	case "717":
		return RouteType_ShareTaxiBus

	case "800":
		return RouteType_TrolleybusService

	case "900":
		return RouteType_TramService
	case "901":
		return RouteType_CityTram
	case "902":
		return RouteType_LocalTram
	case "903":
		return RouteType_RegionalTram
	case "904":
		return RouteType_SightseeingTram
	case "905":
		return RouteType_ShuttleTram
	case "906":
		return RouteType_AllTramServices
	case "907":
		return RouteType_CrossborderTram

	case "1000":
		return RouteType_WaterTransportService
	case "1001":
		return RouteType_InternationalCarFerry
	case "1002":
		return RouteType_NationalCarFerry
	case "1003":
		return RouteType_RegionalCarFerry
	case "1004":
		return RouteType_LocalCarFerry
	case "1005":
		return RouteType_InternationalPassengerFerry
	case "1006":
		return RouteType_NationalPassengerFerry
	case "1007":
		return RouteType_RegionalPassengerFerry
	case "1008":
		return RouteType_LocalPassengerFerry
	case "1009":
		return RouteType_PostBoat
	case "1010":
		return RouteType_TrainFerry
	case "1011":
		return RouteType_RoadLinkFerry
	case "1012":
		return RouteType_AirportLinkFerry
	case "1013":
		return RouteType_CarHighSpeedFerry
	case "1014":
		return RouteType_PassengerHighSpeedFerry
	case "1015":
		return RouteType_SightseeingBoat
	case "1016":
		return RouteType_SchoolBoat
	case "1017":
		return RouteType_CableDrawnBoat
	case "1018":
		return RouteType_RiverBus
	case "1019":
		return RouteType_ScheduledFerry
	case "1020":
		return RouteType_ShuttleFerry
	case "1021":
		return RouteType_AllWaterTransportServices

	case "1100":
		return RouteType_AirService
	case "1101":
		return RouteType_InternationalAirService
	case "1102":
		return RouteType_DomesticAirService
	case "1103":
		return RouteType_IntercontinentalAirService
	case "1104":
		return RouteType_DomesticScheduledAirService
	case "1105":
		return RouteType_ShuttleAirService
	case "1106":
		return RouteType_IntercontinentalCharterAir
	case "1107":
		return RouteType_InternationalCharterAir
	case "1108":
		return RouteType_RoundTripCharterAir
	case "1109":
		return RouteType_SightseeingAirService
	case "1110":
		return RouteType_HelicopterAirService
	case "1111":
		return RouteType_DomesticCharterAirService
	case "1112":
		return RouteType_AllAirServices

	case "1200":
		return RouteType_FerryService

	case "1300":
		return RouteType_AerialLiftService
	case "1301":
		return RouteType_TelecabinService
	case "1302":
		return RouteType_CableCarService
	case "1303":
		return RouteType_ElevatorService
	case "1304":
		return RouteType_ChairLiftService
	case "1305":
		return RouteType_DragLiftService
	case "1306":
		return RouteType_SmallTelecabin
	case "1307":
		return RouteType_AllTelecabinServices

	case "1400":
		return RouteType_FunicularService
	case "1401":
		return RouteType_Funicular_1
	case "1402":
		return RouteType_AllFunicularService

	case "1501":
		return RouteType_CommunalTaxi
	case "1502":
		return RouteType_WaterTaxi
	case "1503":
		return RouteType_RailTaxi
	case "1504":
		return RouteType_BikeTaxi
	case "1505":
		return RouteType_LicensedTaxi
	case "1506":
		return RouteType_PrivateHireVehicle
	case "1507":
		return RouteType_AllTaxiServices

	case "1700":
		return RouteType_MiscellaneousService
	case "1701":
		return RouteType_CableCarMisc
	case "1702":
		return RouteType_HorseDrawnCarriage

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
	case RouteType_RailwayService:
		return "RAILWAY_SERVICE"
	case RouteType_HighSpeedRailService:
		return "HIGH_SPEED_RAIL_SERVICE"
	case RouteType_LongDistanceRailService:
		return "LONG_DISTANCE_RAIL_SERVICE"
	case RouteType_InterRegionalRailService:
		return "INTER_REGIONAL_RAIL_SERVICE"
	case RouteType_CarTransportRailService:
		return "CAR_TRANSPORT_RAIL_SERVICE"
	case RouteType_SleeperRailService:
		return "SLEEPER_RAIL_SERVICE"
	case RouteType_RegionalRailService:
		return "REGIONAL_RAIL_SERVICE"
	case RouteType_TouristRailwayService:
		return "TOURIST_RAILWAY_SERVICE"
	case RouteType_RailShuttle:
		return "RAIL_SHUTTLE"
	case RouteType_SuburbanRailway:
		return "SUBURBAN_RAILWAY"
	case RouteType_ReplacementRailService:
		return "REPLACEMENT_RAIL_SERVICE"
	case RouteType_SpecialRailService:
		return "SPECIAL_RAIL_SERVICE"
	case RouteType_LorryTransportRailService:
		return "LORRY_TRANSPORT_RAIL_SERVICE"
	case RouteType_AllRailServices:
		return "ALL_RAIL_SERVICES"
	case RouteType_CrossCountryRailService:
		return "CROSS_COUNTRY_RAIL_SERVICE"
	case RouteType_VehicleTransportRailService:
		return "VEHICLE_TRANSPORT_RAIL_SERVICE"
	case RouteType_RackAndPinionRailway:
		return "RACK_AND_PINION_RAILWAY"
	case RouteType_AdditionalRailService:
		return "ADDITIONAL_RAIL_SERVICE"
	case RouteType_CoachService:
		return "COACH_SERVICE"
	case RouteType_InternationalCoach:
		return "INTERNATIONAL_COACH"
	case RouteType_NationalCoach:
		return "NATIONAL_COACH"
	case RouteType_ShuttleCoach:
		return "SHUTTLE_COACH"
	case RouteType_RegionalCoach:
		return "REGIONAL_COACH"
	case RouteType_SpecialCoach:
		return "SPECIAL_COACH"
	case RouteType_TouristCoach:
		return "TOURIST_COACH"
	case RouteType_CommuterCoach:
		return "COMMUTER_COACH"
	case RouteType_AllCoachServices:
		return "ALL_COACH_SERVICES"
	case RouteType_SuburbanCoachService:
		return "SUBURBAN_COACH_SERVICE"
	case RouteType_UrbanRailwayService:
		return "URBAN_RAILWAY_SERVICE"
	case RouteType_MetroService:
		return "METRO_SERVICE"
	case RouteType_UndergroundService:
		return "UNDERGROUND_SERVICE"
	case RouteType_UrbanRailway:
		return "URBAN_RAILWAY"
	case RouteType_AllUrbanRailwayServices:
		return "ALL_URBAN_RAILWAY_SERVICES"
	case RouteType_UrbanMonorail:
		return "URBAN_MONORAIL"
	case RouteType_BusService:
		return "BUS_SERVICE"
	case RouteType_RegionalBus:
		return "REGIONAL_BUS"
	case RouteType_ExpressBus:
		return "EXPRESS_BUS"
	case RouteType_StoppingBus:
		return "STOPPING_BUS"
	case RouteType_LocalBus:
		return "LOCAL_BUS"
	case RouteType_NightBus:
		return "NIGHT_BUS"
	case RouteType_PostBus:
		return "POST_BUS"
	case RouteType_SpecialNeedsBus:
		return "SPECIAL_NEEDS_BUS"
	case RouteType_MobilityBus:
		return "MOBILITY_BUS"
	case RouteType_MobilityBusRegisteredDisabled:
		return "MOBILITY_BUS_REGISTERED_DISABLED"
	case RouteType_SightseeingBus:
		return "SIGHTSEEING_BUS"
	case RouteType_ShuttleBus:
		return "SHUTTLE_BUS"
	case RouteType_SchoolBus:
		return "SCHOOL_BUS"
	case RouteType_SchoolPublicServiceBus:
		return "SCHOOL_PUBLIC_SERVICE_BUS"
	case RouteType_RailReplacementBus:
		return "RAIL_REPLACEMENT_BUS"
	case RouteType_DemandResponseBus:
		return "DEMAND_RESPONSE_BUS"
	case RouteType_AllBusServices:
		return "ALL_BUS_SERVICES"
	case RouteType_ShareTaxiBus:
		return "SHARE_TAXI_BUS"
	case RouteType_TrolleybusService:
		return "TROLLEY_BUS_SERVICE"
	case RouteType_TramService:
		return "TRAM_SERVICE"
	case RouteType_CityTram:
		return "CITY_TRAM"
	case RouteType_LocalTram:
		return "LOCAL_TRAM"
	case RouteType_RegionalTram:
		return "REGIONAL_TRAM"
	case RouteType_SightseeingTram:
		return "SIGHTSEEING_TRAM"
	case RouteType_ShuttleTram:
		return "SHUTTLE_TRAM"
	case RouteType_AllTramServices:
		return "ALL_TRAM_SERVICES"
	case RouteType_CrossborderTram:
		return "CROSSBORDER_TRAM"
	case RouteType_WaterTransportService:
		return "WATER_TRANSPORT_SERVICE"
	case RouteType_InternationalCarFerry:
		return "INTERNATIONAL_CAR_FERRY"
	case RouteType_NationalCarFerry:
		return "NATIONAL_CAR_FERRY"
	case RouteType_RegionalCarFerry:
		return "REGIONAL_CAR_FERRY"
	case RouteType_LocalCarFerry:
		return "LOCAL_CAR_FERRY"
	case RouteType_InternationalPassengerFerry:
		return "INTERNATIONAL_PASSENGER_FERRY"
	case RouteType_NationalPassengerFerry:
		return "NATIONAL_PASSENGER_FERRY"
	case RouteType_RegionalPassengerFerry:
		return "REGIONAL_PASSENGER_FERRY"
	case RouteType_LocalPassengerFerry:
		return "LOCAL_PASSENGER_FERRY"
	case RouteType_PostBoat:
		return "POST_BOAT"
	case RouteType_TrainFerry:
		return "TRAIN_FERRY"
	case RouteType_RoadLinkFerry:
		return "ROAD_LINK_FERRY"
	case RouteType_AirportLinkFerry:
		return "AIRPORT_LINK_FERRY"
	case RouteType_CarHighSpeedFerry:
		return "CAR_HIGH_SPEED_FERRY"
	case RouteType_PassengerHighSpeedFerry:
		return "PASSENGER_HIGH_SPEED_FERRY"
	case RouteType_SightseeingBoat:
		return "SIGHTSEEING_BOAT"
	case RouteType_SchoolBoat:
		return "SCHOOL_BOAT"
	case RouteType_CableDrawnBoat:
		return "CABLE_DRAWN_BOAT"
	case RouteType_RiverBus:
		return "RIVER_BUS"
	case RouteType_ScheduledFerry:
		return "SCHEDULED_FERRY"
	case RouteType_ShuttleFerry:
		return "SHUTTLE_FERRY"
	case RouteType_AllWaterTransportServices:
		return "ALL_WATER_TRANSPORT_SERVICES"
	case RouteType_AirService:
		return "AIR_SERVICE"
	case RouteType_InternationalAirService:
		return "INTERNATIONAL_AIR_SERVICE"
	case RouteType_DomesticAirService:
		return "DOMESTIC_AIR_SERVICE"
	case RouteType_IntercontinentalAirService:
		return "INTERCONTINENTAL_AIR_SERVICE"
	case RouteType_DomesticScheduledAirService:
		return "DOMESTIC_SCHEDULED_AIR_SERVICE"
	case RouteType_ShuttleAirService:
		return "SHUTTLE_AIR_SERVICE"
	case RouteType_IntercontinentalCharterAir:
		return "INTERCONTINENTAL_CHARTER_AIR"
	case RouteType_InternationalCharterAir:
		return "INTERNATIONAL_CHARTER_AIR"
	case RouteType_RoundTripCharterAir:
		return "ROUND_TRIP_CHARTER_AIR"
	case RouteType_SightseeingAirService:
		return "SIGHTSEEING_AIR_SERVICE"
	case RouteType_HelicopterAirService:
		return "HELICOPTER_AIR_SERVICE"
	case RouteType_DomesticCharterAirService:
		return "DOMESTIC_CHARTER_AIR_SERVICE"
	case RouteType_AllAirServices:
		return "ALL_AIR_SERVICES"
	case RouteType_FerryService:
		return "FERRY_SERVICE"
	case RouteType_AerialLiftService:
		return "AERIAL_LIFT_SERVICE"
	case RouteType_TelecabinService:
		return "TELECABIN_SERVICE"
	case RouteType_CableCarService:
		return "CABLE_CAR_SERVICE"
	case RouteType_ElevatorService:
		return "ELEVATOR_SERVICE"
	case RouteType_ChairLiftService:
		return "CHAIR_LIFT_SERVICE"
	case RouteType_DragLiftService:
		return "DRAG_LIFT_SERVICE"
	case RouteType_SmallTelecabin:
		return "SMALL_TELECABIN"
	case RouteType_AllTelecabinServices:
		return "ALL_TELECABIN_SERVICES"
	case RouteType_FunicularService:
		return "FUNICULAR_SERVICE"
	case RouteType_AllFunicularService:
		return "ALL_FUNICULAR_SERVICE"
	case RouteType_TaxiService:
		return "TAXI_SERVICE"
	case RouteType_CommunalTaxi:
		return "COMMUNAL_TAXI"
	case RouteType_WaterTaxi:
		return "WATER_TAXI"
	case RouteType_RailTaxi:
		return "RAIL_TAXI"
	case RouteType_BikeTaxi:
		return "BIKE_TAXI"
	case RouteType_LicensedTaxi:
		return "LICENSED_TAXI"
	case RouteType_PrivateHireVehicle:
		return "PRIVATE_HIRE_VEHICLE"
	case RouteType_AllTaxiServices:
		return "ALL_TAXI_SERVICES"
	case RouteType_MiscellaneousService:
		return "MISCELLANEOUS_SERVICE"
	case RouteType_CableCarMisc:
		return "CABLE_CAR_MISC"
	case RouteType_HorseDrawnCarriage:
		return "HORSE_DRAWN_CARRIAGE"
	case RouteType_Unknown:
		return "UNKNOWN"
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

// PaymentMethod describes the payment method for a fare.
type PaymentMethod int

const (
	PaymentMethod_Onboard        PaymentMethod = 0
	PaymentMethod_BeforeBoarding PaymentMethod = 1
)

func parsePaymentMethod(s string) (PaymentMethod, error) {
	switch s {
	case "0":
		return PaymentMethod_Onboard, nil
	case "1":
		return PaymentMethod_BeforeBoarding, nil
	default:
		return -1, fmt.Errorf("invalid payment method: %s", s)
	}
}

func (p PaymentMethod) String() string {
	switch p {
	case PaymentMethod_BeforeBoarding:
		return "BEFORE_BOARDING"
	case PaymentMethod_Onboard:
		return "ONBOARD"
	default:
		return "UNKNOWN"
	}
}

type FareTransferPolicy int

const (
	FareTransferPolicy_NoTransfer   FareTransferPolicy = 0
	FareTransferPolicy_OneTransfer  FareTransferPolicy = 1
	FareTransferPolicy_TwoTransfers FareTransferPolicy = 2
	FareTransferPolicy_Unlimited    FareTransferPolicy = 3
)

func parseFareTransferPolicy(s string) (FareTransferPolicy, error) {
	switch strings.TrimSpace(s) {
	case "0":
		return FareTransferPolicy_NoTransfer, nil
	case "1":
		return FareTransferPolicy_OneTransfer, nil
	case "2":
		return FareTransferPolicy_TwoTransfers, nil
	case "":
		return FareTransferPolicy_Unlimited, nil
	default:
		return -1, fmt.Errorf("invalid fare transfer policy: %s", s)
	}
}
