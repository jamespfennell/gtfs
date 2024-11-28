package gtfs

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/jamespfennell/gtfs/extensions"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
	"google.golang.org/protobuf/proto"
)

// Realtime contains the parsed content for a single GTFS realtime message.
type Realtime struct {
	CreatedAt time.Time

	Trips []Trip

	Vehicles []Vehicle

	Alerts []Alert
}

type Trip struct {
	ID              TripID
	StopTimeUpdates []StopTimeUpdate

	Vehicle *Vehicle

	IsEntityInMessage bool
}

func (trip *Trip) GetVehicle() Vehicle {
	if trip != nil && trip.Vehicle != nil {
		return *trip.Vehicle
	}
	return Vehicle{}
}

type TripScheduleRelationship = gtfsrt.TripDescriptor_ScheduleRelationship

type TripID struct {
	ID          string
	RouteID     string
	DirectionID DirectionID

	HasStartTime bool
	StartTime    time.Duration

	HasStartDate bool
	StartDate    time.Time

	ScheduleRelationship TripScheduleRelationship
}

// Define ordering on trip ids for test consistency
func (t1 TripID) Less(t2 TripID) bool {
	if t1.ID != t2.ID {
		return t1.ID < t2.ID
	}
	if t1.RouteID != t2.RouteID {
		return t1.RouteID < t2.RouteID
	}
	if t1.DirectionID != t2.DirectionID {
		return t1.DirectionID < t2.DirectionID
	}
	if t1.HasStartTime != t2.HasStartTime {
		return !t1.HasStartTime && t2.HasStartTime
	}
	if t1.HasStartTime && t1.StartTime != t2.StartTime {
		return t1.StartTime < t2.StartTime
	}
	if t1.HasStartDate != t2.HasStartDate {
		return !t1.HasStartDate && t2.HasStartDate
	}
	if t1.HasStartDate && !t1.StartDate.Equal(t2.StartDate) {
		return t1.StartDate.Before(t2.StartDate)
	}
	return t1.ScheduleRelationship < t2.ScheduleRelationship
}

type StopTimeUpdateScheduleRelationship = gtfsrt.TripUpdate_StopTimeUpdate_ScheduleRelationship

// TODO: shouldn't this just be StopTime?
type StopTimeUpdate struct {
	StopSequence         *uint32
	StopID               *string
	Arrival              *StopTimeEvent
	Departure            *StopTimeEvent
	NyctTrack            *string
	ScheduleRelationship StopTimeUpdateScheduleRelationship
}

func (stopTimeUpdate *StopTimeUpdate) GetArrival() StopTimeEvent {
	if stopTimeUpdate != nil && stopTimeUpdate.Arrival != nil {
		return *stopTimeUpdate.Arrival
	}
	return StopTimeEvent{}
}

func (stopTimeUpdate *StopTimeUpdate) GetDeparture() StopTimeEvent {
	if stopTimeUpdate != nil && stopTimeUpdate.Departure != nil {
		return *stopTimeUpdate.Departure
	}
	return StopTimeEvent{}
}

type StopTimeEvent struct {
	Time        *time.Time
	Delay       *time.Duration
	Uncertainty *int32
}

type VehicleID struct {
	ID           string
	Label        string
	LicensePlate string
}

type Position struct {
	// Degrees North, in the WGS-84 coordinate system.
	Latitude *float32
	// Degrees East, in the WGS-84 coordinate system.
	Longitude *float32
	// Bearing, in degrees, clockwise from North, i.e., 0 is North and 90 is East.
	// This can be the compass bearing, or the direction towards the next stop
	// or intermediate location.
	// This should not be direction deduced from the sequence of previous
	// positions, which can be computed from previous data.
	Bearing *float32
	// Odometer value, in meters.
	Odometer *float64
	// Momentary speed measured by the vehicle, in meters per second.
	Speed *float32
}

type CurrentStatus = gtfsrt.VehiclePosition_VehicleStopStatus
type CongestionLevel = gtfsrt.VehiclePosition_CongestionLevel
type OccupancyStatus = gtfsrt.VehiclePosition_OccupancyStatus

type Vehicle struct {
	ID *VehicleID

	Trip *Trip

	Position *Position

	CurrentStopSequence *uint32

	StopID *string

	CurrentStatus *CurrentStatus

	Timestamp *time.Time

	CongestionLevel CongestionLevel

	OccupancyStatus *OccupancyStatus

	OccupancyPercentage *uint32

	IsEntityInMessage bool
}

func (vehicle *Vehicle) GetID() VehicleID {
	if vehicle != nil && vehicle.ID != nil {
		return *vehicle.ID
	}
	return VehicleID{}
}
func (vehicle *Vehicle) GetTrip() Trip {
	if vehicle != nil && vehicle.Trip != nil {
		return *vehicle.Trip
	}
	return Trip{}
}

type Alert struct {
	ID               string
	Cause            AlertCause
	Effect           AlertEffect
	ActivePeriods    []AlertActivePeriod
	InformedEntities []AlertInformedEntity
	Header           []AlertText
	Description      []AlertText
	URL              []AlertText
}

type AlertCause = gtfsrt.Alert_Cause

const (
	UnknownCause     AlertCause = gtfsrt.Alert_UNKNOWN_CAUSE
	OtherCause       AlertCause = gtfsrt.Alert_OTHER_CAUSE
	TechnicalProblem AlertCause = gtfsrt.Alert_TECHNICAL_PROBLEM
	Strike           AlertCause = gtfsrt.Alert_STRIKE
	Demonstration    AlertCause = gtfsrt.Alert_DEMONSTRATION
	Accident         AlertCause = gtfsrt.Alert_ACCIDENT
	Holiday          AlertCause = gtfsrt.Alert_HOLIDAY
	Weather          AlertCause = gtfsrt.Alert_WEATHER
	Maintenance      AlertCause = gtfsrt.Alert_MAINTENANCE
	Construction     AlertCause = gtfsrt.Alert_CONSTRUCTION
	PoliceActivity   AlertCause = gtfsrt.Alert_POLICE_ACTIVITY
	MedicalEmergency AlertCause = gtfsrt.Alert_MEDICAL_EMERGENCY
)

type AlertEffect = gtfsrt.Alert_Effect

const (
	UnknownEffect      AlertEffect = gtfsrt.Alert_UNKNOWN_EFFECT
	NoService          AlertEffect = gtfsrt.Alert_NO_SERVICE
	ReducedService     AlertEffect = gtfsrt.Alert_REDUCED_SERVICE
	SignificantDelays  AlertEffect = gtfsrt.Alert_SIGNIFICANT_DELAYS
	Detour             AlertEffect = gtfsrt.Alert_DETOUR
	AdditionalService  AlertEffect = gtfsrt.Alert_ADDITIONAL_SERVICE
	ModifiedService    AlertEffect = gtfsrt.Alert_MODIFIED_SERVICE
	OtherEffect        AlertEffect = gtfsrt.Alert_OTHER_EFFECT
	StopMoved          AlertEffect = gtfsrt.Alert_STOP_MOVED
	NoEffect           AlertEffect = gtfsrt.Alert_NO_EFFECT
	AccessibilityIssue AlertEffect = gtfsrt.Alert_ACCESSIBILITY_ISSUE
)

type AlertActivePeriod struct {
	StartsAt *time.Time
	EndsAt   *time.Time
}

type AlertInformedEntity struct {
	AgencyID *string
	RouteID  *string
	// If RouteType isn't set, this will be RouteType_Unknown.
	RouteType   RouteType
	DirectionID DirectionID
	TripID      *TripID
	StopID      *string
}

type AlertText struct {
	Text     string
	Language string
}

type ParseRealtimeOptions struct {
	// The timezone to interpret date field.
	//
	// It can be nil, in which case UTC will used.
	Timezone *time.Location

	// The GTFS Realtime extension to use when parsing.
	//
	// This can be nil, in which case no extension is used.
	Extension extensions.Extension
}

func (opts *ParseRealtimeOptions) timezoneOrUTC() *time.Location {
	if opts.Timezone != nil {
		return opts.Timezone
	}
	return time.UTC
}

func ParseRealtime(content []byte, opts *ParseRealtimeOptions) (*Realtime, error) {
	if opts.Extension == nil {
		opts.Extension = extensions.NoExtension()
	}
	feedMessage := &gtfsrt.FeedMessage{}
	if err := proto.Unmarshal(content, feedMessage); err != nil {
		return nil, fmt.Errorf("failed to parse input as a GTFS Realtime message: %s", err)
	}
	var result Realtime
	if t := feedMessage.GetHeader().Timestamp; t != nil {
		createdAt := time.Unix(int64(*t), 0).In(opts.timezoneOrUTC())
		result.CreatedAt = createdAt
	}

	shouldSkip := make([]bool, len(feedMessage.GetEntity()))
	for i, entity := range feedMessage.Entity {
		if tripUpdate := entity.GetTripUpdate(); tripUpdate != nil {
			r := opts.Extension.UpdateTrip(tripUpdate, feedMessage.GetHeader().GetTimestamp())
			shouldSkip[i] = r.ShouldSkip
		} else if vehiclePosition := entity.GetVehicle(); vehiclePosition != nil {
			opts.Extension.UpdateVehicle(vehiclePosition)
		} else if alert := entity.Alert; alert != nil {
			shouldSkip[i] = opts.Extension.UpdateAlert(entity.Id, alert)
		}
	}

	tripsById := map[TripID]*Trip{}
	vehiclesByID := map[VehicleID]*Vehicle{}
	tripIDToVehicleID := map[TripID]VehicleID{}
	vehicleIDToTripID := map[VehicleID]TripID{}
	vehiclesWithNoID := []Vehicle{}
	for i, entity := range feedMessage.Entity {
		if shouldSkip[i] {
			continue
		}
		var trip *Trip
		var vehicle *Vehicle
		var alert *Alert
		var alertTrips []Trip
		var ok bool

		if tripUpdate := entity.TripUpdate; tripUpdate != nil {
			trip, vehicle, ok = parseTripUpdate(tripUpdate, opts)
		} else if vehiclePosition := entity.Vehicle; vehiclePosition != nil {
			trip, vehicle = parseVehicle(vehiclePosition, opts)
			ok = true
		} else if entityAlert := entity.Alert; entityAlert != nil {
			alert, alertTrips = parseAlert(entity.GetId(), entityAlert, opts)
			ok = true
		} else {
			continue
		}

		if !ok {
			continue
		}

		if alert != nil {
			result.Alerts = append(result.Alerts, *alert)
			for _, trip := range alertTrips {
				if _, ok := tripsById[trip.ID]; !ok {
					tripsById[trip.ID] = &Trip{}
				}
				mergeTrip(tripsById[trip.ID], trip)
			}
		}
		if trip != nil {
			if _, ok := tripsById[trip.ID]; !ok {
				tripsById[trip.ID] = &Trip{}
			}
			mergeTrip(tripsById[trip.ID], *trip)
		}
		if vehicle != nil {
			if vehicle.ID != nil {
				if _, ok := vehiclesByID[*vehicle.ID]; !ok {
					vehiclesByID[*vehicle.ID] = &Vehicle{}
				}
				mergeVehicle(vehiclesByID[*vehicle.ID], *vehicle)
			} else {
				vehiclesWithNoID = append(vehiclesWithNoID, *vehicle)
			}
		}
		if trip != nil && vehicle != nil {
			if vehicle.ID != nil {
				// TODO: what if these already exist?
				// Maybe we should also return a Diagnostics message
				tripIDToVehicleID[trip.ID] = *vehicle.ID
				vehicleIDToTripID[*vehicle.ID] = trip.ID
			} else {
				trip.Vehicle = vehicle
			}
		}
	}

	for tripID, trip := range tripsById {
		if vehicleID, ok := tripIDToVehicleID[tripID]; ok {
			trip.Vehicle = vehiclesByID[vehicleID]
		}
		result.Trips = append(result.Trips, *trip)
	}

	sort.Slice(result.Trips, func(i, j int) bool {
		return result.Trips[i].ID.Less(result.Trips[j].ID)
	})

	for vehicleID, vehicle := range vehiclesByID {
		if tripID, ok := vehicleIDToTripID[vehicleID]; ok {
			vehicle.Trip = tripsById[tripID]
		}
		result.Vehicles = append(result.Vehicles, *vehicle)
	}
	result.Vehicles = append(result.Vehicles, vehiclesWithNoID...)
	return &result, nil
}

func parseTripUpdate(tripUpdate *gtfsrt.TripUpdate, opts *ParseRealtimeOptions) (*Trip, *Vehicle, bool) {
	if tripUpdate.Trip == nil {
		return nil, nil, false
	}
	trip := &Trip{
		ID:                parseTripDescriptor(tripUpdate.Trip, opts),
		IsEntityInMessage: true,
	}
	convertStopTimeEvent := func(stopTimeEvent *gtfsrt.TripUpdate_StopTimeEvent) *StopTimeEvent {
		if stopTimeEvent == nil {
			return nil
		}
		result := StopTimeEvent{
			Uncertainty: stopTimeEvent.Uncertainty,
		}
		if stopTimeEvent.Time != nil {
			t := time.Unix(*stopTimeEvent.Time, 0).In(opts.timezoneOrUTC())
			result.Time = &t
		}
		if stopTimeEvent.Delay != nil {
			d := time.Duration(*stopTimeEvent.Delay) * time.Second
			result.Delay = &d
		}
		return &result
	}
	for _, stopTimeUpdate := range tripUpdate.StopTimeUpdate {
		trip.StopTimeUpdates = append(trip.StopTimeUpdates, StopTimeUpdate{
			StopSequence:         stopTimeUpdate.StopSequence,
			StopID:               stopTimeUpdate.StopId,
			Arrival:              convertStopTimeEvent(stopTimeUpdate.Arrival),
			Departure:            convertStopTimeEvent(stopTimeUpdate.Departure),
			NyctTrack:            opts.Extension.GetTrack(stopTimeUpdate),
			ScheduleRelationship: stopTimeUpdate.GetScheduleRelationship(),
		})
	}
	if tripUpdate.Vehicle == nil {
		return trip, nil, true
	}
	vehicle := &Vehicle{
		ID:                parseVehicleDescriptor(tripUpdate.Vehicle),
		IsEntityInMessage: false,
	}
	return trip, vehicle, true
}

func parseVehicle(vehiclePosition *gtfsrt.VehiclePosition, opts *ParseRealtimeOptions) (*Trip, *Vehicle) {
	var congestionLevel = gtfsrt.VehiclePosition_UNKNOWN_CONGESTION_LEVEL
	if vehiclePosition.CongestionLevel != nil {
		congestionLevel = *vehiclePosition.CongestionLevel
	}
	vehicle := &Vehicle{
		ID:                  parseVehicleDescriptor(vehiclePosition.Vehicle),
		Position:            convertVehiclePosition(vehiclePosition),
		CurrentStopSequence: vehiclePosition.CurrentStopSequence,
		StopID:              vehiclePosition.StopId,
		CurrentStatus:       vehiclePosition.CurrentStatus,
		Timestamp:           convertOptionalTimestamp(vehiclePosition.Timestamp, opts.timezoneOrUTC()),
		CongestionLevel:     congestionLevel,
		OccupancyStatus:     vehiclePosition.OccupancyStatus,
		OccupancyPercentage: vehiclePosition.OccupancyPercentage,
		IsEntityInMessage:   true,
	}
	if vehiclePosition.Trip == nil {
		return nil, vehicle
	}
	trip := &Trip{
		ID:                parseTripDescriptor(vehiclePosition.Trip, opts),
		IsEntityInMessage: false,
	}
	return trip, vehicle
}

func convertVehiclePosition(vehiclePosition *gtfsrt.VehiclePosition) *Position {
	if vehiclePosition == nil {
		return nil
	}
	p := vehiclePosition.Position
	if p == nil {
		return nil
	}
	return &Position{
		Latitude:  p.Latitude,
		Longitude: p.Longitude,
		Bearing:   p.Bearing,
		Odometer:  p.Odometer,
		Speed:     p.Speed,
	}
}

func mergeTrip(t *Trip, new Trip) {
	t.ID = new.ID
	if !new.IsEntityInMessage {
		return
	}
	*t = new
}

func mergeVehicle(v *Vehicle, new Vehicle) {
	v.ID = new.ID
	if !new.IsEntityInMessage {
		return
	}
	*v = new
}

var startTimeRegex *regexp.Regexp = regexp.MustCompile(`^([0-9]{2}):([0-9]{2}):([0-9]{2})$`)
var startDateRegex *regexp.Regexp = regexp.MustCompile(`^([0-9]{4})([0-9]{2})([0-9]{2})$`)

func parseOptionalTripDescriptor(tripDesc *gtfsrt.TripDescriptor, opts *ParseRealtimeOptions) *TripID {
	if tripDesc == nil {
		return nil
	}
	tripID := parseTripDescriptor(tripDesc, opts)
	return &tripID
}

func parseTripDescriptor(tripDesc *gtfsrt.TripDescriptor, opts *ParseRealtimeOptions) TripID {
	id := TripID{
		ID:                   tripDesc.GetTripId(),
		RouteID:              tripDesc.GetRouteId(),
		DirectionID:          parseDirectionID_GTFSRealtime(tripDesc.DirectionId),
		ScheduleRelationship: tripDesc.GetScheduleRelationship(),
	}
	id.HasStartTime, id.StartTime = parseStartTime(tripDesc.StartTime)
	id.HasStartDate, id.StartDate = parseStartDate(tripDesc.StartDate, opts.timezoneOrUTC())
	return id
}

// parseStartTime parses a start time of the form HH:MM:SS into a Duration.
//
// It does not handle daylight saving time currently.
func parseStartTime(startTime *string) (bool, time.Duration) {
	if startTime == nil {
		return false, 0
	}
	startTimeMatch := startTimeRegex.FindStringSubmatch(*startTime)
	if startTimeMatch == nil {
		return false, 0
	}
	h, _ := strconv.Atoi(startTimeMatch[1])
	m, _ := strconv.Atoi(startTimeMatch[2])
	s, _ := strconv.Atoi(startTimeMatch[3])
	return true, time.Duration((h*60+m)*60+s) * time.Second
}

func parseStartDate(startDate *string, timezone *time.Location) (bool, time.Time) {
	if startDate == nil {
		return false, time.Time{}
	}
	startDateMatch := startDateRegex.FindStringSubmatch(*startDate)
	if startDateMatch == nil {
		return false, time.Time{}
	}
	y, _ := strconv.Atoi(startDateMatch[1])
	m, _ := strconv.Atoi(startDateMatch[2])
	d, _ := strconv.Atoi(startDateMatch[3])
	return true, time.Date(y, time.Month(m), d, 0, 0, 0, 0, timezone)
}

func parseVehicleDescriptor(vehicleDesc *gtfsrt.VehicleDescriptor) *VehicleID {
	if vehicleDesc == nil {
		return nil
	}
	valOrEmpty := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	vehicleID := VehicleID{
		ID:           valOrEmpty(vehicleDesc.Id),
		Label:        valOrEmpty(vehicleDesc.Label),
		LicensePlate: valOrEmpty(vehicleDesc.LicensePlate),
	}
	var zeroVehicleID VehicleID
	if vehicleID == zeroVehicleID {
		return nil
	}
	return &vehicleID
}

func parseAlert(ID string, alert *gtfsrt.Alert, opts *ParseRealtimeOptions) (*Alert, []Trip) {
	var activePeriods []AlertActivePeriod
	for _, entity := range alert.GetActivePeriod() {
		activePeriods = append(activePeriods, AlertActivePeriod{
			StartsAt: convertOptionalTimestamp(entity.Start, opts.timezoneOrUTC()),
			EndsAt:   convertOptionalTimestamp(entity.End, opts.timezoneOrUTC()),
		})
	}
	var informedEntities []AlertInformedEntity
	var trips []Trip
	var informedRoutes = make(map[string]bool)
	var informedRoutesFromTripIDs = make(map[string]map[DirectionID]bool)
	for _, entity := range alert.GetInformedEntity() {
		tripIDOrNil := parseOptionalTripDescriptor(entity.Trip, opts)

		// Handle the case where the trip descriptor's doesn't map to a trip instance but the route ID is not.
		// In such cases, we can inform the route in one or both directions.
		// Such cases are not handled by the GTFS-realtime spec, but occur in some feeds such
		// as the MTA bus alerts feed. See: https://groups.google.com/g/mtadeveloperresources/c/pn_EupBj1nY
		if tripIDOrNil != nil && !tripIDUniquelyIdentifiesTrip(tripIDOrNil) && tripIDOrNil.RouteID != "" {
			if tripIDOrNil.DirectionID == DirectionID_Unspecified {
				informedRoutesFromTripIDs[tripIDOrNil.RouteID] = map[DirectionID]bool{
					DirectionID_False: true,
					DirectionID_True:  true,
				}
			} else {
				if _, ok := informedRoutesFromTripIDs[tripIDOrNil.RouteID]; !ok {
					informedRoutesFromTripIDs[tripIDOrNil.RouteID] = map[DirectionID]bool{}
				}
				informedRoutesFromTripIDs[tripIDOrNil.RouteID][tripIDOrNil.DirectionID] = true
			}
		}
		if entity.RouteId != nil {
			informedRoutes[*entity.RouteId] = true
		}

		informedEntity := AlertInformedEntity{
			AgencyID:    entity.AgencyId,
			RouteID:     entity.RouteId,
			RouteType:   parseRouteType_GTFSRealtime(entity.RouteType),
			DirectionID: parseDirectionID_GTFSRealtime(entity.DirectionId),
			TripID:      tripIDOrNil,
			StopID:      entity.StopId,
		}

		// Ensure at least one entity is informed
		if !alertInformedEntityInformsAtLeastOneEntity(informedEntity) {
			continue
		}

		if tripIDUniquelyIdentifiesTrip(tripIDOrNil) {
			trips = append(trips, Trip{
				ID:                *tripIDOrNil,
				IsEntityInMessage: false,
			})
		} else {
			// Clear the trip ID if it doesn't uniquely identify a trip
			informedEntity.TripID = nil
		}

		informedEntities = append(informedEntities, informedEntity)
	}

	for routeID, directions := range informedRoutesFromTripIDs {
		if informedRoutes[routeID] {
			continue
		}

		routeIDCopy := routeID
		if directions[DirectionID_False] && directions[DirectionID_True] {
			// Inform the route in both directions
			informedEntities = append(informedEntities, AlertInformedEntity{
				RouteID:   &routeIDCopy,
				RouteType: RouteType_Unknown,
			})
		} else {
			// Inform the route in one direction
			var informedDirection DirectionID
			if directions[DirectionID_False] {
				informedDirection = DirectionID_False
			} else {
				informedDirection = DirectionID_True
			}

			informedEntities = append(informedEntities, AlertInformedEntity{
				RouteID:     &routeIDCopy,
				RouteType:   RouteType_Unknown,
				DirectionID: informedDirection,
			})
		}
	}

	gtfsAlert := &Alert{
		ID:               ID,
		ActivePeriods:    activePeriods,
		Cause:            alert.GetCause(),
		Effect:           alert.GetEffect(),
		InformedEntities: informedEntities,
		Header:           buildAlertText(alert.GetHeaderText()),
		Description:      buildAlertText(alert.GetDescriptionText()),
		URL:              buildAlertText(alert.GetUrl()),
	}
	return gtfsAlert, trips
}

func buildAlertText(ts *gtfsrt.TranslatedString) []AlertText {
	var texts []AlertText
	for _, s := range ts.GetTranslation() {
		texts = append(texts, AlertText{
			Text:     s.GetText(),
			Language: s.GetLanguage(),
		})
	}
	return texts
}

func convertOptionalTimestamp(in *uint64, timezone *time.Location) *time.Time {
	if in == nil {
		return nil
	}
	out := time.Unix(int64(*in), 0).In(timezone)
	return &out
}

func alertInformedEntityInformsAtLeastOneEntity(alertInformedEntity AlertInformedEntity) bool {
	return (alertInformedEntity.AgencyID != nil || alertInformedEntity.RouteID != nil ||
		alertInformedEntity.RouteType != RouteType_Unknown || tripIDUniquelyIdentifiesTrip(alertInformedEntity.TripID) ||
		alertInformedEntity.StopID != nil)
}

func tripIDUniquelyIdentifiesTrip(tripID *TripID) bool {
	return tripID != nil &&
		(tripID.ID != "" || (tripID.RouteID != "" && tripID.DirectionID != DirectionID_Unspecified && tripID.HasStartTime && tripID.HasStartDate))
}
