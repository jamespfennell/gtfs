// Package gtfs contains parsers for GTFS static and realtime feeds.
package gtfs

import (
	"archive/zip"
	"bytes"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/jamespfennell/gtfs/csv"
)

// Static contains the parsed content for a single GTFS static message.
type Static struct {
	Agencies  []Agency
	Routes    []Route
	Stops     []Stop
	Transfers []Transfer
	Services  []Service
	Trips     []ScheduledTrip
	Shapes    []Shape
}

// Agency corresponds to a single row in the agency.txt file.
type Agency struct {
	Id       string
	Name     string
	Url      string
	Timezone string
	Language *string
	Phone    *string
	FareUrl  *string
	Email    *string
}

type RouteType int32

const (
	Tram       RouteType = 0
	Subway     RouteType = 1
	Rail       RouteType = 2
	Bus        RouteType = 3
	Ferry      RouteType = 4
	CableTram  RouteType = 5
	AerialLift RouteType = 6
	Funicular  RouteType = 7
	TrolleyBus RouteType = 11
	Monorail   RouteType = 12

	UnknownRouteType RouteType = 10000
)

func NewRouteType(i int) (RouteType, bool) {
	var t RouteType
	switch i {
	case 0:
		t = Tram
	case 1:
		t = Subway
	case 2:
		t = Rail
	case 3:
		t = Bus
	case 4:
		t = Ferry
	case 5:
		t = CableTram
	case 6:
		t = AerialLift
	case 7:
		t = Funicular
	case 11:
		t = TrolleyBus
	case 12:
		t = Monorail
	default:
		return UnknownRouteType, false
	}
	return t, true
}

func (t RouteType) String() string {
	switch t {
	case Tram:
		return "TRAM"
	case Subway:
		return "SUBWAY"
	case Rail:
		return "RAIL"
	case Bus:
		return "BUS"
	case Ferry:
		return "FERRY"
	case CableTram:
		return "CABLE_TRAM"
	case AerialLift:
		return "AERIAL_LIFT"
	case Funicular:
		return "FUNICULAR"
	case TrolleyBus:
		return "TROLLEY_BUS"
	case Monorail:
		return "MONORAIL"
	}
	return "UNKNOWN"
}

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

type Route struct {
	Id                string
	Agency            *Agency
	Color             string
	TextColor         string
	ShortName         *string
	LongName          *string
	Description       *string
	Type              RouteType
	Url               *string
	SortOrder         *int32
	ContinuousPickup  PickupDropOffPolicy
	ContinuousDropOff PickupDropOffPolicy
}

type Stop struct {
	Id                 string
	Code               *string
	Name               *string
	Description        *string
	ZoneId             *string
	Longitude          *float64
	Latitude           *float64
	Url                *string
	Type               StopType
	Parent             *Stop
	Timezone           *string
	WheelchairBoarding WheelchairBoarding
	PlatformCode       *string
}

func (stop *Stop) Root() *Stop {
	for {
		if stop.Parent == nil {
			return stop
		}
		stop = stop.Parent
	}
}

type StopType int32

const (
	Platform       StopType = 0
	Station        StopType = 1
	EntranceOrExit StopType = 2
	GenericNode    StopType = 3
	BoardingArea   StopType = 4
)

func NewStopType(i int) (StopType, bool) {
	var t StopType
	switch i {
	case 0:
		t = Platform
	case 1:
		t = Station
	case 2:
		t = EntranceOrExit
	case 3:
		t = GenericNode
	case 4:
		t = BoardingArea
	default:
		return Platform, false
	}
	return t, true
}

func (t StopType) String() string {
	switch t {
	case Platform:
		return "PLATFORM"
	case Station:
		return "STATION"
	case EntranceOrExit:
		return "ENTRANCE_OR_EXIT"
	case GenericNode:
		return "GENERIC_NODE"
	case BoardingArea:
		return "BOARDING_AREA"
	}
	return "UNKNOWN"
}

type WheelchairBoarding int32

const (
	NotSpecified WheelchairBoarding = 0
	Possible     WheelchairBoarding = 1
	NotPossible  WheelchairBoarding = 2
)

func NewWheelchairBoarding(i int) (WheelchairBoarding, bool) {
	var t WheelchairBoarding
	switch i {
	case 0:
		t = NotSpecified
	case 1:
		t = Possible
	case 2:
		t = NotPossible
	default:
		return NotSpecified, false
	}
	return t, true
}

func (w WheelchairBoarding) String() string {
	switch w {
	case Possible:
		return "POSSIBLE"
	case NotPossible:
		return "NOT_POSSIBLE"
	}
	return "NOT_SPECIFIED"
}

type BikesAllowed int32

const (
	BikesAllowed_NotSpecified BikesAllowed = 0
	BikesAllowed_Allowed      BikesAllowed = 1
	BikesAllowed_NotAllowed   BikesAllowed = 2
)

func (b BikesAllowed) String() string {
	switch b {
	case BikesAllowed_Allowed:
		return "ALLOWED"
	case BikesAllowed_NotAllowed:
		return "NOT_ALLOWED"
	}
	return "NOT_SPECIFIED"
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

type Transfer struct {
	From            *Stop
	To              *Stop
	Type            TransferType
	MinTransferTime *int32
}

type TransferType int32

const (
	Recommended         TransferType = 0
	Timed               TransferType = 1
	RequiresTime        TransferType = 2
	TransferNotPossible TransferType = 3
)

func NewTransferType(i int32) (TransferType, bool) {
	var t TransferType
	switch i {
	case 0:
		t = Recommended
	case 1:
		t = Timed
	case 2:
		t = RequiresTime
	case 3:
		t = TransferNotPossible
	default:
		return Recommended, false
	}
	return t, true
}

func (t TransferType) String() string {
	switch t {
	case Recommended:
		return "RECOMMENDED"
	case Timed:
		return "TIMED"
	case RequiresTime:
		return "REQUIRES_TIME"
	case TransferNotPossible:
		return "TRANSFER_NOT_POSSIBLE"
	}
	return "UNKNOWN"
}

type Service struct {
	Id           string
	Monday       bool
	Tuesday      bool
	Wednesday    bool
	Thursday     bool
	Friday       bool
	Saturday     bool
	Sunday       bool
	StartDate    time.Time
	EndDate      time.Time
	AddedDates   []time.Time
	RemovedDates []time.Time
}

type ScheduledTrip struct {
	Route                *Route
	Service              *Service
	ID                   string
	Headsign             *string
	ShortName            *string
	DirectionId          DirectionID
	BlockID              *string
	WheelchairAccessible WheelchairBoarding
	BikesAllowed         BikesAllowed
	StopTimes            []ScheduledStopTime
	Shape                *Shape
	Frequencies          []Frequency
}

type ScheduledStopTime struct {
	Trip                  *ScheduledTrip
	Stop                  *Stop
	ArrivalTime           time.Duration
	DepartureTime         time.Duration
	StopSequence          int
	Headsign              *string
	PickupType            PickupDropOffPolicy
	DropOffType           PickupDropOffPolicy
	ContinuousPickup      PickupDropOffPolicy
	ContinuousDropOff     PickupDropOffPolicy
	ShapeDistanceTraveled *float64
	ExactTimes            bool
}

type ShapePoint struct {
	Latitude  float64
	Longitude float64
	Distance  *float64
}

type Shape struct {
	ID     string
	Points []ShapePoint
}

type ExactTimes int32

const (
	FrequencyBased ExactTimes = 0
	ScheduleBased  ExactTimes = 1
)

func NewExactTimes(i int) ExactTimes {
	switch i {
	case 1:
		return ScheduleBased
	case 0:
		fallthrough
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

type Frequency struct {
	StartTime  time.Duration
	EndTime    time.Duration
	Headway    time.Duration
	ExactTimes ExactTimes
}

// SortScheduledStopTimes sorts the provided stop times based on the stop sequence field.
func SortScheduledStopTimes(stopTimes []ScheduledStopTime) {
	sort.Sort(scheduledStopTimeSorter(stopTimes))
}

type scheduledStopTimeSorter []ScheduledStopTime

func (s scheduledStopTimeSorter) Len() int {
	return len([]ScheduledStopTime(s))
}

func (s scheduledStopTimeSorter) Less(i, j int) bool {
	return s[i].StopSequence < s[j].StopSequence
}

func (s scheduledStopTimeSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type ParseStaticOptions struct{}

// ParseStatic parses the content as a GTFS static feed.
func ParseStatic(content []byte, opts ParseStaticOptions) (*Static, error) {
	reader, err := zip.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return nil, err
	}
	result := &Static{}
	fileNameToFile := map[string]*zip.File{}
	for _, file := range reader.File {
		fileNameToFile[file.Name] = file
	}
	serviceIdToService := map[string]Service{}
	shapeIdToShape := map[string]*Shape{}
	tripIdToScheduledTrip := map[string]*ScheduledTrip{}
	timezone := time.UTC
	for _, table := range []struct {
		File     string
		Action   func(file *csv.File)
		Optional bool
	}{
		{
			File: "agency.txt",
			Action: func(file *csv.File) {
				result.Agencies = parseAgencies(file)
				if len(result.Agencies) > 0 {
					var err error
					timezone, err = time.LoadLocation(result.Agencies[0].Timezone)
					if err != nil {
						timezone = time.UTC
					}
				}
			},
		},
		{
			File: "routes.txt",
			Action: func(file *csv.File) {
				result.Routes = parseRoutes(file, result.Agencies)
			},
		},
		{
			File: "stops.txt",
			Action: func(file *csv.File) {
				result.Stops = parseStops(file)
			},
		},
		{
			File: "transfers.txt",
			Action: func(file *csv.File) {
				if file != nil {
					result.Transfers = parseTransfers(file, result.Stops)
				}
			},
			Optional: true,
		},
		{
			File: "calendar.txt",
			Action: func(file *csv.File) {
				if file != nil {
					parseCalendar(file, serviceIdToService, timezone)
				}
			},
			Optional: true,
		},
		{
			File: "calendar_dates.txt",
			Action: func(file *csv.File) {
				if file != nil {
					parseCalendarDates(file, serviceIdToService, timezone)
				}
				for _, service := range serviceIdToService {
					result.Services = append(result.Services, service)
				}
			},
			Optional: true,
		},
		{
			File: "shapes.txt",
			Action: func(file *csv.File) {
				result.Shapes = parseShapes(file)
				for idx, shape := range result.Shapes {
					shapeIdToShape[shape.ID] = &result.Shapes[idx]
				}
			},
			Optional: true,
		},
		{
			File: "trips.txt",
			Action: func(file *csv.File) {
				result.Trips = parseScheduledTrips(file, result.Routes, result.Services, shapeIdToShape)
				for idx, trip := range result.Trips {
					tripIdToScheduledTrip[trip.ID] = &result.Trips[idx]
				}
			},
		},
		{
			File: "frequencies.txt",
			Action: func(file *csv.File) {
				parseFrequencies(file, tripIdToScheduledTrip)
			},
			Optional: true,
		},
		{
			File: "stop_times.txt",
			Action: func(file *csv.File) {
				parseScheduledStopTimes(file, result.Stops, result.Trips)
			},
		},
	} {
		zipFile := fileNameToFile[table.File]
		if zipFile == nil {
			if table.Optional {
				// TODO: this is a bit hacky. Maybe have a table.PostAction() that is invoked instead?
				table.Action(nil)
				continue
			}
			return nil, fmt.Errorf("no %q file in GTFS static feed", table.File)
		}
		file, err := readCsvFile(zipFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read %q: %w", table.File, err)
		}
		table.Action(file)
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("failed to read %q: %w", table.File, err)
		}
	}

	return result, nil
}

func readCsvFile(zipFile *zip.File) (*csv.File, error) {
	content, err := zipFile.Open()
	if err != nil {
		return nil, err
	}
	f, err := csv.New(content)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func parseAgencies(csv *csv.File) []Agency {
	idColumn := csv.OptionalColumn("agency_id")
	nameColumn := csv.RequiredColumn("agency_name")
	urlColumn := csv.RequiredColumn("agency_url")
	timezoneColumn := csv.RequiredColumn("agency_timezone")
	languageColumn := csv.OptionalColumn("agency_lang")
	phoneColumn := csv.OptionalColumn("agency_phone")
	fareUrlColumn := csv.OptionalColumn("agency_fare_url")
	emailColumn := csv.OptionalColumn("agency_email")

	if err := csv.MissingRequiredColumns(); err != nil {
		fmt.Println(err)
		return nil
	}

	var agencies []Agency
	for csv.NextRow() {
		agency := Agency{
			// TODO: support specifying the default agency ID in the GTFS static parser settings
			Id:       idColumn.ReadOr(fmt.Sprintf("%s_id", nameColumn.Read())),
			Name:     nameColumn.Read(),
			Url:      urlColumn.Read(),
			Timezone: timezoneColumn.Read(),
			Language: languageColumn.Read(),
			Phone:    phoneColumn.Read(),
			FareUrl:  fareUrlColumn.Read(),
			Email:    emailColumn.Read(),
		}
		if missingKeys := csv.MissingRowKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping agency %+v because of missing keys %s", agency, missingKeys)
			continue
		}
		agencies = append(agencies, agency)
	}
	return agencies
}

func parseRoutes(csv *csv.File, agencies []Agency) []Route {
	idColumn := csv.RequiredColumn("route_id")
	agencyIDColumn := csv.OptionalColumn("agency_id")
	colorColumn := csv.OptionalColumn("route_color")
	textColorColumn := csv.OptionalColumn("route_text_color")
	shortNameColumn := csv.OptionalColumn("route_short_name")
	longNameColumn := csv.OptionalColumn("route_long_name")
	descriptionColumn := csv.OptionalColumn("route_desc")
	routeTypeColumn := csv.RequiredColumn("route_type")
	urlColumn := csv.OptionalColumn("route_url")
	sortOrderColumn := csv.OptionalColumn("route_sort_order")
	continuousPickupColumn := csv.OptionalColumn("continuous_pickup")
	continuousDropOffColumn := csv.OptionalColumn("continuous_drop_off")

	if err := csv.MissingRequiredColumns(); err != nil {
		fmt.Println(err)
		return nil
	}

	var routes []Route
	for csv.NextRow() {
		routeID := idColumn.Read()
		agencyID := agencyIDColumn.Read()
		var agency *Agency
		if agencyID != nil {
			for i := range agencies {
				if agencies[i].Id == *agencyID {
					agency = &agencies[i]
					break
				}
			}
			if agency == nil {
				log.Printf("skipping route %s: no match for agency ID %s", routeID, *agencyID)
				continue
			}
		} else if len(agencies) == 1 {
			// In GTFS static if there is a single agency, a route's agency ID field can be omitted in
			// which case the route's agency is the unique agency in the feed.
			agency = &agencies[0]
		} else {
			log.Printf("skipping route %s: no agency ID provided but no unique agency", routeID)
			continue
		}
		route := Route{
			Id:                routeID,
			Agency:            agency,
			Color:             colorColumn.ReadOr("FFFFFF"),
			TextColor:         textColorColumn.ReadOr("000000"),
			ShortName:         shortNameColumn.Read(),
			LongName:          longNameColumn.Read(),
			Description:       descriptionColumn.Read(),
			Type:              parseRouteType(routeTypeColumn.Read()),
			Url:               urlColumn.Read(),
			SortOrder:         parseRouteSortOrder(sortOrderColumn.Read()),
			ContinuousPickup:  parseRoutePolicy(continuousPickupColumn.ReadOr("")),
			ContinuousDropOff: parseRoutePolicy(continuousDropOffColumn.ReadOr("")),
		}
		if missingKeys := csv.MissingRowKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping route %+v because of missing keys %s", route, missingKeys)
			continue
		}
		routes = append(routes, route)
	}
	return routes
}

func parseRouteType(raw string) RouteType {
	i, err := strconv.Atoi(raw)
	if err != nil {
		return UnknownRouteType
	}
	t, _ := NewRouteType(i)
	return t
}

func parseRouteSortOrder(raw *string) *int32 {
	if raw == nil {
		return nil
	}
	i, err := strconv.Atoi(*raw)
	if err != nil {
		return nil
	}
	i32 := int32(i)
	return &i32
}

func parseRoutePolicy(s string) PickupDropOffPolicy {
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

func parseStops(csv *csv.File) []Stop {
	idColumn := csv.RequiredColumn("stop_id")
	codeColumn := csv.OptionalColumn("stop_code")
	nameColumn := csv.OptionalColumn("stop_name")
	descriptionColumn := csv.OptionalColumn("stop_desc")
	zoneIdColumn := csv.OptionalColumn("zone_id")
	longitudeColumn := csv.OptionalColumn("stop_lon")
	latitudeColumn := csv.OptionalColumn("stop_lat")
	urlColumn := csv.OptionalColumn("stop_url")
	typeColumn := csv.OptionalColumn("location_type")
	timezoneColumn := csv.OptionalColumn("stop_timezone")
	wheelchairBoardingColumn := csv.OptionalColumn("wheelchair_boarding")
	platformCodeColumn := csv.OptionalColumn("platform_code")
	parentStationColumn := csv.OptionalColumn("parent_station")

	if err := csv.MissingRequiredColumns(); err != nil {
		fmt.Println(err)
		return nil
	}

	var stops []Stop
	stopIdToIndex := map[string]int{}
	stopIdToParent := map[string]string{}
	for csv.NextRow() {
		stop := Stop{
			Id:                 idColumn.Read(),
			Code:               codeColumn.Read(),
			Name:               nameColumn.Read(),
			Description:        descriptionColumn.Read(),
			ZoneId:             zoneIdColumn.Read(),
			Longitude:          parseFloat64(longitudeColumn.Read()),
			Latitude:           parseFloat64(latitudeColumn.Read()),
			Url:                urlColumn.Read(),
			Type:               parseStopType(typeColumn.Read()),
			Timezone:           timezoneColumn.Read(),
			WheelchairBoarding: parseWheelchairBoarding(wheelchairBoardingColumn.Read()),
			PlatformCode:       platformCodeColumn.Read(),
		}
		if missingKeys := csv.MissingRowKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping stop %+v because of missing keys %s", stop, missingKeys)
			continue
		}
		stopIdToIndex[stop.Id] = len(stops)
		if parentStopId := parentStationColumn.Read(); parentStopId != nil {
			stopIdToParent[stop.Id] = *parentStopId
		}
		stops = append(stops, stop)
	}
	for stopId, parentStopId := range stopIdToParent {
		parentStopIndex, ok := stopIdToIndex[parentStopId]
		if !ok {
			continue
		}
		stops[stopIdToIndex[stopId]].Parent = &stops[parentStopIndex]
	}
	return stops
}

func parseFloat64(raw *string) *float64 {
	if raw == nil {
		return nil
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(*raw), 64)
	if err != nil {
		return nil
	}
	return &f
}

func parseStopType(raw *string) StopType {
	if raw == nil {
		return Platform
	}
	i, err := strconv.Atoi(*raw)
	if err != nil {
		return Platform
	}
	t, _ := NewStopType(i)
	return t
}

func parseWheelchairBoarding(raw *string) WheelchairBoarding {
	if raw == nil {
		return NotSpecified
	}
	i, err := strconv.Atoi(*raw)
	if err != nil {
		return NotSpecified
	}
	t, _ := NewWheelchairBoarding(i)
	return t
}

func parseTransfers(csv *csv.File, stops []Stop) []Transfer {
	fromStopIDColumn := csv.RequiredColumn("from_stop_id")
	toStopIDColumn := csv.RequiredColumn("to_stop_id")
	typeColumn := csv.OptionalColumn("transfer_type")
	transferTimeColumn := csv.OptionalColumn("min_transfer_time")

	if err := csv.MissingRequiredColumns(); err != nil {
		fmt.Println(err)
		return nil
	}

	stopIdToStop := map[string]*Stop{}
	for i := range stops {
		stopIdToStop[stops[i].Id] = &stops[i]
	}
	var transfers []Transfer
	for csv.NextRow() {
		fromStopID := fromStopIDColumn.Read()
		toStopID := toStopIDColumn.Read()
		if missingKeys := csv.MissingRowKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping transfer because of missing keys %s", missingKeys)
			continue
		}
		fromStop, fromStopOk := stopIdToStop[fromStopID]
		toStop, toStopOk := stopIdToStop[toStopID]
		if !fromStopOk {
			log.Printf("Skipping transfer because from_stop_id %q is invalid", fromStopID)
			continue
		}
		if !toStopOk {
			log.Printf("Skipping transfer because to_stop_id %q is invalid", toStopID)
			continue
		}
		if fromStop.Id == toStop.Id {
			// log.Printf("Skipping transfer between the same stop %q", fromStop.Id)
			continue
		}
		transfers = append(transfers, Transfer{
			From:            fromStop,
			To:              toStop,
			Type:            parseTransferType(typeColumn.Read()),
			MinTransferTime: parseInt32(transferTimeColumn.Read()),
		})
	}
	return transfers
}

func parseTransferType(raw *string) TransferType {
	i := parseInt32(raw)
	if i == nil {
		return Recommended
	}
	t, _ := NewTransferType(*i)
	return t
}

func parseInt32(raw *string) *int32 {
	if raw == nil {
		return nil
	}
	i, err := strconv.ParseInt(*raw, 10, 32)
	if err != nil {
		return nil
	}
	i32 := int32(i)
	return &i32
}

func parseCalendar(f *csv.File, m map[string]Service, timezone *time.Location) {
	startDateColumn := f.RequiredColumn("start_date")
	endDateColumn := f.RequiredColumn("end_date")
	serviceIDColumn := f.RequiredColumn("service_id")
	var dayColumns [7]csv.RequiredColumn
	for i, days := range []string{
		"monday",
		"tuesday",
		"wednesday",
		"thursday",
		"friday",
		"saturday",
		"sunday",
	} {
		dayColumns[i] = f.RequiredColumn(days)
	}

	if err := f.MissingRequiredColumns(); err != nil {
		fmt.Println(err)
		return
	}

	parseBool := func(s string) bool {
		return s == "1"
	}
	for f.NextRow() {
		startDate, err := parseTime(startDateColumn.Read(), timezone)
		if err != nil {
			continue
		}
		endDate, err := parseTime(endDateColumn.Read(), timezone)
		if err != nil {
			continue
		}
		service := Service{
			Id:        serviceIDColumn.Read(),
			Monday:    parseBool(dayColumns[0].Read()),
			Tuesday:   parseBool(dayColumns[1].Read()),
			Wednesday: parseBool(dayColumns[2].Read()),
			Thursday:  parseBool(dayColumns[3].Read()),
			Friday:    parseBool(dayColumns[4].Read()),
			Saturday:  parseBool(dayColumns[5].Read()),
			Sunday:    parseBool(dayColumns[6].Read()),
			StartDate: startDate,
			EndDate:   endDate,
		}
		if missingKeys := f.MissingRowKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping calendar because of missing keys %s", missingKeys)
			continue
		}
		m[service.Id] = service
	}
}

func parseCalendarDates(csv *csv.File, m map[string]Service, timezone *time.Location) {
	serviceIDColumn := csv.RequiredColumn("service_id")
	dateColumn := csv.RequiredColumn("date")
	exceptionTypeColumn := csv.RequiredColumn("exception_type")

	if err := csv.MissingRequiredColumns(); err != nil {
		fmt.Println(err)
		return
	}

	for csv.NextRow() {
		serviceId := serviceIDColumn.Read()
		date, err := parseTime(dateColumn.Read(), timezone)
		if err != nil {
			continue
		}
		exceptionType := exceptionTypeColumn.Read()
		if missingKeys := csv.MissingRowKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping calendar because of missing keys %s", missingKeys)
			continue
		}
		service, ok := m[serviceId]
		service.Id = serviceId
		if !ok {
			service.StartDate = date
			service.EndDate = date
		} else {
			if date.Before(service.StartDate) {
				service.StartDate = date
			}
			if service.EndDate.Before(date) {
				service.EndDate = date
			}
		}
		switch exceptionType {
		case "1":
			service.AddedDates = append(service.AddedDates, date)
		case "2":
			service.RemovedDates = append(service.RemovedDates, date)
		default:
			continue
		}
		m[service.Id] = service
	}
}

func parseTime(s string, timezone *time.Location) (time.Time, error) {
	return time.ParseInLocation("20060102", s, timezone)
}

func parseDirectionID(s string) DirectionID {
	switch s {
	case "0":
		return DirectionIDFalse
	case "1":
		return DirectionIDTrue
	default:
		return DirectionIDUnspecified
	}
}

func parseScheduledTrips(csv *csv.File, routes []Route, services []Service, shapeIDToShape map[string]*Shape) []ScheduledTrip {
	routeIDColumn := csv.RequiredColumn("route_id")
	serviceIDColumn := csv.RequiredColumn("service_id")
	tripIDColumn := csv.RequiredColumn("trip_id")
	tripHeadsignColumn := csv.OptionalColumn("trip_headsign")
	tripShortNameColumn := csv.OptionalColumn("trip_short_name")
	directionIDColumn := csv.OptionalColumn("direction_id")
	blockIDColumn := csv.OptionalColumn("block_id")
	wheelchairAccessibleColumn := csv.OptionalColumn("wheelchair_accessible")
	bikesAllowedColumn := csv.OptionalColumn("bikes_allowed")
	shapeIDColumn := csv.OptionalColumn("shape_id")

	if err := csv.MissingRequiredColumns(); err != nil {
		fmt.Println(err)
		return nil
	}

	idToService := map[string]*Service{}
	for i := range services {
		idToService[services[i].Id] = &services[i]
	}
	idToRoute := map[string]*Route{}
	for i := range routes {
		idToRoute[routes[i].Id] = &routes[i]
	}
	var trips []ScheduledTrip
	for csv.NextRow() {
		trip := ScheduledTrip{
			Route:                idToRoute[routeIDColumn.Read()],
			Service:              idToService[serviceIDColumn.Read()],
			ID:                   tripIDColumn.Read(),
			Headsign:             tripHeadsignColumn.Read(),
			ShortName:            tripShortNameColumn.Read(),
			DirectionId:          parseDirectionID(directionIDColumn.ReadOr("")),
			BlockID:              blockIDColumn.Read(),
			WheelchairAccessible: parseWheelchairBoarding(wheelchairAccessibleColumn.Read()),
			BikesAllowed:         parseBikesAllowed(bikesAllowedColumn.ReadOr("")),
		}

		shapeIDOrNil := shapeIDColumn.Read()
		if shapeIDOrNil != nil {
			if shape, ok := shapeIDToShape[*shapeIDOrNil]; ok {
				trip.Shape = shape
			} else {
				log.Printf("Shape %s not found for trip %s", *shapeIDOrNil, trip.ID)
			}
		}

		if missingKeys := csv.MissingRowKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping trip because of missing keys %s", missingKeys)
			continue
		}
		if trip.Route == nil {
			log.Print("Skipping trip because of missing route")
			continue
		}
		if trip.Service == nil {
			log.Print("Skipping trip because of missing service")
			continue
		}
		trips = append(trips, trip)
	}
	return trips
}

func parseScheduledStopTimes(csv *csv.File, stops []Stop, trips []ScheduledTrip) {
	stopIDColumn := csv.RequiredColumn("stop_id")
	stopSequenceKey := csv.RequiredColumn("stop_sequence")
	tripIDColumn := csv.RequiredColumn("trip_id")
	arrivalTimeColumn := csv.OptionalColumn("arrival_time")
	departureTimeColumn := csv.OptionalColumn("departure_time")
	stopHeadsignColumn := csv.OptionalColumn("stop_headsign")
	pickupTypeColumn := csv.OptionalColumn("pickup_type")
	dropOffTypeColumn := csv.OptionalColumn("drop_off_type")
	continuousPickupColumn := csv.OptionalColumn("continuous_pickup")
	continuousDropOffColumn := csv.OptionalColumn("continuous_drop_off")
	shapeDistanceTraveledColumn := csv.OptionalColumn("shape_dist_traveled")
	timepointColumn := csv.OptionalColumn("timepoint")
	if err := csv.MissingRequiredColumns(); err != nil {
		fmt.Println(err)
		return
	}

	idToStop := map[string]*Stop{}
	for i := range stops {
		idToStop[stops[i].Id] = &stops[i]
	}
	idToTrip := map[string]*ScheduledTrip{}
	for i := range trips {
		idToTrip[trips[i].ID] = &trips[i]
	}
	var currentTrip *ScheduledTrip
	var currentTripID string
	for csv.NextRow() {
		arrival, arrivalOk := parseGtfsTimeToDuration(arrivalTimeColumn.Read())
		departure, departureOk := parseGtfsTimeToDuration(departureTimeColumn.Read())
		if !arrivalOk && !departureOk {
			continue
		}
		if !departureOk {
			arrival = departure
		}
		if !arrivalOk {
			departure = arrival
		}
		stopSequence, err := strconv.Atoi(stopSequenceKey.Read())
		if err != nil {
			// TODO: log a warning
			continue
		}
		stopTime := ScheduledStopTime{
			Stop:                  idToStop[stopIDColumn.Read()],
			Headsign:              stopHeadsignColumn.Read(),
			ArrivalTime:           arrival,
			StopSequence:          stopSequence,
			DepartureTime:         departure,
			PickupType:            parseRoutePolicy(pickupTypeColumn.ReadOr("")),
			DropOffType:           parseRoutePolicy(dropOffTypeColumn.ReadOr("")),
			ContinuousPickup:      parseRoutePolicy(continuousPickupColumn.ReadOr("")),
			ContinuousDropOff:     parseRoutePolicy(continuousDropOffColumn.ReadOr("")),
			ShapeDistanceTraveled: parseFloat64(shapeDistanceTraveledColumn.Read()),
			ExactTimes:            timepointColumn.ReadOr("1") == "1",
		}
		tripID := tripIDColumn.Read()
		if currentTrip == nil || currentTripID != tripID {
			thisTrip := idToTrip[tripID]
			if currentTrip != nil && cap(thisTrip.StopTimes) == 0 {
				thisTrip.StopTimes = make([]ScheduledStopTime, 0, len(currentTrip.StopTimes))
			}
			currentTrip = thisTrip
			currentTripID = tripID
		}
		if missingKeys := csv.MissingRowKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping stop time because of missing keys %s", missingKeys)
			continue
		}
		if stopTime.Stop == nil {
			continue
		}
		if currentTrip == nil {
			continue
		}
		currentTrip.StopTimes = append(currentTrip.StopTimes, stopTime)
	}
	for _, trip := range idToTrip {
		SortScheduledStopTimes(trip.StopTimes)
	}
}

func parseGtfsTimeToDuration(s *string) (time.Duration, bool) {
	if s == nil {
		return 0, false
	}
	var pieces [3]int
	var i int
	for _, c := range *s {
		if '0' <= c && c <= '9' {
			pieces[i] = 10*pieces[i] + int(c-'0')
		} else if c == ':' {
			i++
			if i > 2 {
				return 0, false
			}
		} else if unicode.IsSpace(c) {
			continue
		} else {
			return 0, false
		}
	}
	hours := pieces[0]
	minutes := pieces[1]
	seconds := pieces[2]
	return time.Duration((hours*60+minutes)*60+seconds) * time.Second, true
}

type ShapeRow struct {
	ShapePtLat        float64
	ShapePtLon        float64
	ShapePtSequence   int32
	ShapeDistTraveled *float64
}

func parseShapes(csv *csv.File) []Shape {
	if csv == nil {
		return nil
	}

	shapeIDColumn := csv.RequiredColumn("shape_id")
	shapePtLatColumn := csv.RequiredColumn("shape_pt_lat")
	shapePtLonColumn := csv.RequiredColumn("shape_pt_lon")
	shapePtSequenceColumn := csv.RequiredColumn("shape_pt_sequence")
	shapeDistTraveled := csv.OptionalColumn("shape_dist_traveled")

	if err := csv.MissingRequiredColumns(); err != nil {
		fmt.Println(err)
		return nil
	}

	shapeIDToRowData := map[string][]ShapeRow{}
	for csv.NextRow() {
		shapeID := shapeIDColumn.Read()
		shapePtLat := parseFloat64(ptr(shapePtLatColumn.Read()))
		shapePtLon := parseFloat64(ptr(shapePtLonColumn.Read()))
		shapePtSequence := parseInt32(ptr(shapePtSequenceColumn.Read()))
		shapeDistTraveled := parseFloat64(shapeDistTraveled.Read())

		if missingKeys := csv.MissingRowKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping shape because of missing keys %s", missingKeys)
			continue
		}

		shapeIDToRowData[shapeID] = append(shapeIDToRowData[shapeID], ShapeRow{
			ShapePtLat:        *shapePtLat,
			ShapePtLon:        *shapePtLon,
			ShapePtSequence:   *shapePtSequence,
			ShapeDistTraveled: shapeDistTraveled,
		})
	}

	shapes := make([]Shape, 0, len(shapeIDToRowData))
	for shapeID, rows := range shapeIDToRowData {
		// Sort the rows by sequence number
		sort.Slice(rows, func(i, j int) bool {
			return rows[i].ShapePtSequence < rows[j].ShapePtSequence
		})

		points := make([]ShapePoint, 0, len(rows))
		for _, row := range rows {
			points = append(points, ShapePoint{
				Latitude:  row.ShapePtLat,
				Longitude: row.ShapePtLon,
				Distance:  row.ShapeDistTraveled,
			})
		}

		shapes = append(shapes, Shape{
			ID:     shapeID,
			Points: points,
		})
	}

	// Sort the shapes by ID
	sort.Slice(shapes, func(i, j int) bool {
		return shapes[i].ID < shapes[j].ID
	})

	return shapes
}

func parseFrequencies(csv *csv.File, tripIDToScheduledTrip map[string]*ScheduledTrip) {
	if csv == nil {
		return
	}

	tripIDColumn := csv.RequiredColumn("trip_id")
	startTimeColumn := csv.RequiredColumn("start_time")
	endTimeColumn := csv.RequiredColumn("end_time")
	headwaySecsColumn := csv.RequiredColumn("headway_secs")
	exactTimesColumn := csv.OptionalColumn("exact_times")

	if err := csv.MissingRequiredColumns(); err != nil {
		fmt.Println(err)
		return
	}

	for csv.NextRow() {
		tripID := tripIDColumn.Read()
		startTime := startTimeColumn.Read()
		endTime := endTimeColumn.Read()
		headwaySecs := headwaySecsColumn.Read()
		exactTimes := exactTimesColumn.Read()

		if missingKeys := csv.MissingRowKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping frequency because of missing keys %s", missingKeys)
			continue
		}
		scheduledTripOrNil := tripIDToScheduledTrip[tripID]
		if scheduledTripOrNil == nil {
			log.Printf("Skipping frequency because of missing trip %s", tripID)
			continue
		}
		headwaySecsOrNil := parseInt32(ptr(headwaySecs))
		if headwaySecsOrNil == nil {
			log.Print("Skipping frequency because of invalid headway_secs")
			continue
		}
		startTimeDuration, startTimeDurationOk := parseGtfsTimeToDuration(&startTime)
		if !startTimeDurationOk {
			log.Print("Skipping frequency because of invalid start_time")
			continue
		}
		endTimeDuration, endTimeDurationOk := parseGtfsTimeToDuration(&endTime)
		if !endTimeDurationOk {
			log.Print("Skipping frequency because of invalid end_time")
			continue
		}

		var exactTimesOrDefault ExactTimes = FrequencyBased
		if exactTimes != nil {
			switch *exactTimes {
			case "":
				fallthrough
			case "0":
				exactTimesOrDefault = FrequencyBased
			case "1":
				exactTimesOrDefault = ScheduleBased
			default:
				log.Print("Skipping frequency because of invalid exact_times")
				continue
			}
		}

		frequency := Frequency{
			StartTime:  startTimeDuration,
			EndTime:    endTimeDuration,
			Headway:    time.Duration(*headwaySecsOrNil) * time.Second,
			ExactTimes: exactTimesOrDefault,
		}

		scheduledTripOrNil.Frequencies = append(scheduledTripOrNil.Frequencies, frequency)
	}
}

func ptr[T any](t T) *T {
	return &t
}
