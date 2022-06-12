package nyctalerts

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jamespfennell/gtfs/extensions"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
	"google.golang.org/protobuf/proto"
)

// ExtensionOpts contains the options for the NYCT alerts extension.
type ExtensionOpts struct {
	// The MTA publishes duplicate alerts for elevator outages. For each elevator and each affected platform
	// (e.g. L03N) there is a separate alert with ID `<stop_id>?#<elevator_id>`.
	//
	// If true, these alerts are consolidated into a single alert with ID `elevator:<elevator_id>`.
	DeduplicateElevatorAlerts bool

	// The MTA's elevator alerts use the platform (e.g. L03N) for the affected entity and in the ID. This makes
	// sense in some cases because an elevator may only serve one platform if the station does not offer a transfer
	// between platforms (for example, the 14th and 6av station). However for regular stations this results in
	// duplicate alerts because both platforms get an alert (e.g., A27N and A27S). In this case it makes sense
	// to use the station ID A27.
	//
	// If true, platform IDs are replaced by station IDs in the alert ID and in the list of effected entities.
	// Alerts for different platforms in the same station are consolidiated.
	UseStationIDsForElevatorAlerts bool

	// When there are no trains running for a route due to the standard timetable (e.g., there are no C trains
	// overnight), the MTA publishes an alert. Arguably this is not really an alert becuase this information is
	// already in the timetable.
	//
	// If true, these alerts are skipped.
	SkipTimetabledNoServiceAlerts bool
}

// Extension returns the NYCT alerts extension with the provided options applied.
func Extension(opts ExtensionOpts) extensions.Extension {
	return extension{
		deduplicateElevatorAlerts:     opts.DeduplicateElevatorAlerts,
		elevatorAlerts:                map[string]*gtfsrt.Alert{},
		skipTimetabledNoServiceAlerts: opts.SkipTimetabledNoServiceAlerts,
	}
}

// Metadata contains some NYCT-specific information on the alert that cannot be mapped to standard GTFS
// realtime alerts feeds.
type Metadata struct {
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
	DisplayBeforeActive       time.Duration
	HumanReadableActivePeriod string
	AffectedEntitiesMetadata  []AffectedEntityMetadata
}

type AffectedEntityMetadata struct {
	SortOrder string
	Priority  gtfsrt.MercuryEntitySelector_Priority
}

type extension struct {
	deduplicateElevatorAlerts bool
	elevatorAlerts            map[string]*gtfsrt.Alert

	skipTimetabledNoServiceAlerts bool

	extensions.NoExtensionImpl
}

var priortyToEffect = map[gtfsrt.MercuryEntitySelector_Priority]gtfsrt.Alert_Effect{
	gtfsrt.MercuryEntitySelector_PRIORITY_NO_SCHEDULED_SERVICE:     gtfsrt.Alert_NO_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_NO_MIDDAY_SERVICE:        gtfsrt.Alert_REDUCED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_NO_OVERNIGHT_SERVICE:     gtfsrt.Alert_REDUCED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_NO_WEEKEND_SERVICE:       gtfsrt.Alert_REDUCED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_WEEKDAY_SCHEDULE:         gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_WEEKEND_SCHEDULE:         gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_SATURDAY_SCHEDULE:        gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_SUNDAY_SCHEDULE:          gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_EXTRA_SERVICE:            gtfsrt.Alert_ADDITIONAL_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_SPECIAL_SCHEDULE:         gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_STATION_NOTICE:           gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_SPECIAL_EVENT:            gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_PLANNED_BOARDING_CHANGE:  gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_BOARDING_CHANGE:          gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_REDUCED_SERVICE:          gtfsrt.Alert_REDUCED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_PLANNED_WORK:             gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_PLANNED_STATIONS_SKIPPED: gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_PLANNED_EXPRESS_TO_LOCAL: gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_SLOW_SPEEDS:              gtfsrt.Alert_SIGNIFICANT_DELAYS,
	gtfsrt.MercuryEntitySelector_PRIORITY_EXPECT_DELAYS:            gtfsrt.Alert_SIGNIFICANT_DELAYS,
	gtfsrt.MercuryEntitySelector_PRIORITY_PLANNED_LOCAL_TO_EXPRESS: gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_PLANNED_BUSES_DETOURED:   gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_PLANNED_TRAINS_REROUTED:  gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_PLANNED_SUBSTITUTE_BUSES: gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_PLANNED_PART_SUSPENDED:   gtfsrt.Alert_REDUCED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_PLANNED_MULTIPLE_CHANGES: gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_SOME_DELAYS:              gtfsrt.Alert_SIGNIFICANT_DELAYS,
	gtfsrt.MercuryEntitySelector_PRIORITY_STATIONS_SKIPPED:         gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_EXPRESS_TO_LOCAL:         gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_DELAYS:                   gtfsrt.Alert_SIGNIFICANT_DELAYS,
	gtfsrt.MercuryEntitySelector_PRIORITY_SOME_REROUTES:            gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_LOCAL_TO_EXPRESS:         gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_BUSES_DETOURED:           gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_SERVICE_CHANGE:           gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_TRAINS_REROUTED:          gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_SUBSTITUTE_BUSES:         gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_PART_SUSPENDED:           gtfsrt.Alert_REDUCED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_MULTIPLE_CHANGES:         gtfsrt.Alert_MODIFIED_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_PLANNED_SUSPENDED:        gtfsrt.Alert_NO_SERVICE,
	gtfsrt.MercuryEntitySelector_PRIORITY_SUSPENDED:                gtfsrt.Alert_NO_SERVICE,
}

var timetabledNoServicePriorities = map[gtfsrt.MercuryEntitySelector_Priority]bool{
	gtfsrt.MercuryEntitySelector_PRIORITY_NO_MIDDAY_SERVICE:    true,
	gtfsrt.MercuryEntitySelector_PRIORITY_NO_OVERNIGHT_SERVICE: true,
	gtfsrt.MercuryEntitySelector_PRIORITY_NO_WEEKEND_SERVICE:   true,
}

func (e extension) UpdateAlert(ID *string, alert *gtfsrt.Alert) bool {
	if e.updateElevatorAlert(ID, alert) {
		return true
	}
	cause := alert.GetCause()
	if strings.HasPrefix(*ID, "lmm:planned_work") {
		cause = gtfsrt.Alert_MAINTENANCE
	} else if strings.HasPrefix(*ID, "lmm:alert") {
		cause = gtfsrt.Alert_TECHNICAL_PROBLEM
		// TODO: search for Police,NYPD,Medical,etc and use different causes?
	}
	alert.Cause = &cause
	if !proto.HasExtension(alert, gtfsrt.E_MercuryAlert) {
		return false
	}
	nyctAlert := proto.GetExtension(alert, gtfsrt.E_MercuryAlert).(*gtfsrt.MercuryAlert)
	_ = nyctAlert
	for _, informedEntity := range alert.GetInformedEntity() {
		priority, ok := getPriorityFromInformedEntity(informedEntity)
		if !ok {
			continue
		}
		if effect, ok := priortyToEffect[priority]; ok {
			alert.Effect = &effect
		}
		if e.skipTimetabledNoServiceAlerts && timetabledNoServicePriorities[priority] {
			return true
		}
	}
	return false
}

func getPriorityFromInformedEntity(informedEntity *gtfsrt.EntitySelector) (gtfsrt.MercuryEntitySelector_Priority, bool) {
	if !proto.HasExtension(informedEntity, gtfsrt.E_MercuryEntitySelector) {
		return 0, false
	}
	extendedEntity := proto.GetExtension(informedEntity, gtfsrt.E_MercuryEntitySelector).(*gtfsrt.MercuryEntitySelector)
	sortOrder := extendedEntity.GetSortOrder()
	i := strings.LastIndex(sortOrder, ":")
	if i < 0 {
		return 0, false
	}
	priorityStr := sortOrder[i+1:]
	priorityInt, err := strconv.Atoi(priorityStr)
	if err != nil {
		return 0, false
	}
	return gtfsrt.MercuryEntitySelector_Priority(priorityInt), true
}

var elevatorAlertIDRegex = regexp.MustCompile("([[:alnum:]]{3}?)[SN]?#EL(.*)")

func (e extension) updateElevatorAlert(ID *string, alert *gtfsrt.Alert) bool {
	match := elevatorAlertIDRegex.FindStringSubmatch(*ID)
	if match == nil {
		return false
	}
	cause := gtfsrt.Alert_MAINTENANCE
	alert.Cause = &cause
	effect := gtfsrt.Alert_ACCESSIBILITY_ISSUE
	alert.Effect = &effect
	if !e.deduplicateElevatorAlerts {
		return false
	}
	stopID := match[1]
	elevatorID := match[2]
	newID := fmt.Sprintf("elevator:EL%s", elevatorID)
	*ID = newID
	deduplicatedAlert, alreadyExists := e.elevatorAlerts[newID]
	if deduplicatedAlert == nil {
		deduplicatedAlert = alert
		deduplicatedAlert.InformedEntity = []*gtfsrt.EntitySelector{}
	}
	deduplicatedAlert.InformedEntity = append(deduplicatedAlert.InformedEntity, &gtfsrt.EntitySelector{
		StopId: &stopID,
	})
	e.elevatorAlerts[newID] = deduplicatedAlert
	return alreadyExists
}
