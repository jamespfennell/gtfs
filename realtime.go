package gtfs

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/jamespfennell/gtfs/extensions"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
	"google.golang.org/protobuf/proto"
)

type DirectionID uint8

const (
	DirectionIDUnspecified DirectionID = 0
	DirectionIDTrue        DirectionID = 1
	DirectionIDFalse       DirectionID = 2
)

func (d DirectionID) String() string {
	if d == DirectionIDTrue {
		return "TRUE"
	}
	if d == DirectionIDFalse {
		return "FALSE"
	}
	return "UNSPECIFIED"
}

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
	NyctIsAssigned  bool

	Vehicle *Vehicle

	IsEntityInMessage bool
}

func (trip *Trip) GetVehicle() Vehicle {
	if trip != nil && trip.Vehicle != nil {
		return *trip.Vehicle
	}
	return Vehicle{}
}

type TripID struct {
	ID          string
	RouteID     string
	DirectionID DirectionID

	HasStartTime bool
	StartTime    time.Duration

	HasStartDate bool
	StartDate    time.Time
}

// TODO: shouldn't this just be StopTime?
type StopTimeUpdate struct {
	StopSequence *uint32
	StopID       *string
	Arrival      *StopTimeEvent
	Departure    *StopTimeEvent
	NyctTrack    *string
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
	LicencePlate string
}

type Vehicle struct {
	ID *VehicleID

	Trip *Trip

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
	AgencyID    *string
	RouteID     *string
	RouteType   *RouteType
	DirectionID *DirectionID
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
		var ok bool

		if tripUpdate := entity.TripUpdate; tripUpdate != nil {
			trip, vehicle, ok = parseTripUpdate(tripUpdate, opts, feedMessage.GetHeader().GetTimestamp())
		} else if vehiclePosition := entity.Vehicle; vehiclePosition != nil {
			trip, vehicle, ok = parseVehicle(vehiclePosition, opts, feedMessage.GetHeader().GetTimestamp())
		} else if alert := entity.Alert; alert != nil {
			result.Alerts = append(result.Alerts, parseAlert(entity.GetId(), alert, opts))
			continue
		} else {
			continue
		}

		if !ok {
			continue
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
	for vehicleID, vehicle := range vehiclesByID {
		if tripID, ok := vehicleIDToTripID[vehicleID]; ok {
			vehicle.Trip = tripsById[tripID]
		}
		result.Vehicles = append(result.Vehicles, *vehicle)
	}
	result.Vehicles = append(result.Vehicles, vehiclesWithNoID...)
	return &result, nil
}

func parseTripUpdate(tripUpdate *gtfsrt.TripUpdate, opts *ParseRealtimeOptions, feedCreatedAt uint64) (*Trip, *Vehicle, bool) {
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
			StopSequence: stopTimeUpdate.StopSequence,
			StopID:       stopTimeUpdate.StopId,
			Arrival:      convertStopTimeEvent(stopTimeUpdate.Arrival),
			Departure:    convertStopTimeEvent(stopTimeUpdate.Departure),
			NyctTrack:    opts.Extension.GetTrack(stopTimeUpdate),
		})
	}
	if tripUpdate.Vehicle == nil {
		return trip, nil, true
	}
	vehicle := &Vehicle{
		ID:                parseVehicleDescriptor(tripUpdate.Vehicle, opts),
		IsEntityInMessage: false,
	}
	return trip, vehicle, true
}

func parseVehicle(vehiclePosition *gtfsrt.VehiclePosition, opts *ParseRealtimeOptions, feedCreatedAt uint64) (*Trip, *Vehicle, bool) {
	vehicle := &Vehicle{
		ID:                parseVehicleDescriptor(vehiclePosition.Vehicle, opts),
		IsEntityInMessage: true,
	}
	if vehiclePosition.Trip == nil {
		return nil, vehicle, true
	}
	trip := &Trip{
		ID:                parseTripDescriptor(vehiclePosition.Trip, opts),
		IsEntityInMessage: false,
	}
	return trip, vehicle, false
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

func parseTripDescriptor(tripDesc *gtfsrt.TripDescriptor, opts *ParseRealtimeOptions) TripID {
	id := TripID{
		ID:          tripDesc.GetTripId(),
		RouteID:     tripDesc.GetRouteId(),
		DirectionID: convertDirectionID(tripDesc.DirectionId),
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

func parseVehicleDescriptor(vehicleDesc *gtfsrt.VehicleDescriptor, opts *ParseRealtimeOptions) *VehicleID {
	vehicleID := VehicleID{
		ID:           vehicleDesc.GetId(),
		Label:        vehicleDesc.GetLabel(),
		LicencePlate: vehicleDesc.GetLicensePlate(),
	}
	var zeroVehicleID VehicleID
	if vehicleID == zeroVehicleID {
		return nil
	}
	return &vehicleID
}

func convertDirectionID(raw *uint32) DirectionID {
	if raw == nil {
		return DirectionIDUnspecified
	}
	if *raw == 0 {
		return DirectionIDFalse
	}
	return DirectionIDTrue
}

func parseAlert(ID string, alert *gtfsrt.Alert, opts *ParseRealtimeOptions) Alert {
	var activePeriods []AlertActivePeriod
	for _, entity := range alert.GetActivePeriod() {
		activePeriods = append(activePeriods, AlertActivePeriod{
			StartsAt: convertOptionalTimestamp(entity.Start, opts.timezoneOrUTC()),
			EndsAt:   convertOptionalTimestamp(entity.End, opts.timezoneOrUTC()),
		})
	}
	var informedEntites []AlertInformedEntity
	for _, entity := range alert.GetInformedEntity() {
		informedEntites = append(informedEntites, AlertInformedEntity{
			AgencyID: entity.AgencyId,
			RouteID:  entity.RouteId,
			// TODO
			// RouteType    *RouteType
			// DirectionID *DirectionID
			// TripID      *TripID
			StopID: entity.StopId,
		})
	}
	return Alert{
		ID:               ID,
		ActivePeriods:    activePeriods,
		Cause:            alert.GetCause(),
		Effect:           alert.GetEffect(),
		InformedEntities: informedEntites,
		Header:           buildAlertText(alert.GetHeaderText()),
		Description:      buildAlertText(alert.GetDescriptionText()),
		URL:              buildAlertText(alert.GetUrl()),
	}
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
