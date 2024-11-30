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

	"github.com/jamespfennell/gtfs/constants"
	"github.com/jamespfennell/gtfs/csv"
	"github.com/jamespfennell/gtfs/warnings"
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

	// Warnings raised during GTFS static parsing.
	Warnings []warnings.StaticWarning
}

// Agency corresponds to a single row in the agency.txt file.
type Agency struct {
	Id       string
	Name     string
	Url      string
	Timezone string
	Language string
	Phone    string
	FareUrl  string
	Email    string
}

// Route corresponds to a single row in the routes.txt file.
type Route struct {
	Id                string
	Agency            *Agency
	Color             string
	TextColor         string
	ShortName         string
	LongName          string
	Description       string
	Type              RouteType
	Url               string
	SortOrder         *int32
	ContinuousPickup  PickupDropOffPolicy
	ContinuousDropOff PickupDropOffPolicy
}

type Stop struct {
	Id                 string
	Code               string
	Name               string
	Description        string
	ZoneId             string
	Longitude          *float64
	Latitude           *float64
	Url                string
	Type               StopType
	Parent             *Stop
	Timezone           string
	WheelchairBoarding WheelchairBoarding
	PlatformCode       string
}

// Root returns the root stop.
func (stop *Stop) Root() *Stop {
	for {
		if stop.Parent == nil {
			return stop
		}
		stop = stop.Parent
	}
}

type Transfer struct {
	From            *Stop
	To              *Stop
	Type            TransferType
	MinTransferTime *int32
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
	Headsign             string
	ShortName            string
	DirectionId          DirectionID
	BlockID              string
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
	Headsign              string
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

type Frequency struct {
	StartTime  time.Duration
	EndTime    time.Duration
	Headway    time.Duration
	ExactTimes ExactTimes
}

type ParseStaticOptions struct {
	// If true, wheelchair boarding information is inherited from parent station
	// when unspecified for a child stop/platform, entrance, or exit.
	InheritWheelchairBoarding bool
}

// ParseStatic parses the content as a GTFS static feed.
func ParseStatic(content []byte, opts ParseStaticOptions) (*Static, error) {
	reader, err := zip.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return nil, err
	}
	result := &Static{}
	fileNameToFile := map[constants.StaticFile]*zip.File{}
	for _, file := range reader.File {
		fileNameToFile[constants.StaticFile(file.Name)] = file
	}
	serviceIdToService := map[string]Service{}
	shapeIdToShape := map[string]*Shape{}
	tripIdToScheduledTrip := map[string]*ScheduledTrip{}
	timezone := time.UTC
	for _, table := range []struct {
		File        constants.StaticFile
		Action      func(file *csv.File) []warnings.StaticWarning
		PostProcess func()
		Optional    bool
	}{
		{
			File: constants.AgencyFile,
			Action: func(file *csv.File) (w []warnings.StaticWarning) {
				result.Agencies, w = parseAgencies(file)
				if len(result.Agencies) > 0 {
					var err error
					timezone, err = time.LoadLocation(result.Agencies[0].Timezone)
					if err != nil {
						timezone = time.UTC
					}
				}
				return
			},
		},
		{
			File: "routes.txt",
			Action: func(file *csv.File) (w []warnings.StaticWarning) {
				result.Routes = parseRoutes(file, result.Agencies)
				return
			},
		},
		{
			File: "stops.txt",
			Action: func(file *csv.File) (w []warnings.StaticWarning) {
				result.Stops = parseStops(file, opts.InheritWheelchairBoarding)
				return
			},
		},
		{
			File: "transfers.txt",
			Action: func(file *csv.File) (w []warnings.StaticWarning) {
				result.Transfers = parseTransfers(file, result.Stops)
				return
			},
			Optional: true,
		},
		{
			File: "calendar.txt",
			Action: func(file *csv.File) (w []warnings.StaticWarning) {
				parseCalendar(file, serviceIdToService, timezone)
				return
			},
			Optional: true,
		},
		{
			File: "calendar_dates.txt",
			Action: func(file *csv.File) (w []warnings.StaticWarning) {
				parseCalendarDates(file, serviceIdToService, timezone)
				return
			},
			PostProcess: func() {
				for _, service := range serviceIdToService {
					result.Services = append(result.Services, service)
				}
			},
			Optional: true,
		},
		{
			File: "shapes.txt",
			Action: func(file *csv.File) (w []warnings.StaticWarning) {
				result.Shapes = parseShapes(file)
				for idx, shape := range result.Shapes {
					shapeIdToShape[shape.ID] = &result.Shapes[idx]
				}
				return
			},
			Optional: true,
		},
		{
			File: "trips.txt",
			Action: func(file *csv.File) (w []warnings.StaticWarning) {
				result.Trips = parseScheduledTrips(file, result.Routes, result.Services, shapeIdToShape)
				for idx, trip := range result.Trips {
					tripIdToScheduledTrip[trip.ID] = &result.Trips[idx]
				}
				return
			},
		},
		{
			File: "frequencies.txt",
			Action: func(file *csv.File) (w []warnings.StaticWarning) {
				parseFrequencies(file, tripIdToScheduledTrip)
				return
			},
			Optional: true,
		},
		{
			File: "stop_times.txt",
			Action: func(file *csv.File) (w []warnings.StaticWarning) {
				parseScheduledStopTimes(file, result.Stops, result.Trips)
				return
			},
		},
	} {
		if table.PostProcess == nil {
			table.PostProcess = func() {}
		}
		zipFile := fileNameToFile[table.File]
		if zipFile == nil {
			if table.Optional {
				table.PostProcess()
				continue
			}
			return nil, fmt.Errorf("no %q file in GTFS static feed", table.File)
		}
		file, err := openCsvFile(table.File, zipFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read %q: %w", table.File, err)
		}
		w := table.Action(file)
		table.PostProcess()
		result.Warnings = append(result.Warnings, w...)
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("failed to read %q: %w", table.File, err)
		}
	}
	return result, nil
}

func openCsvFile(file constants.StaticFile, zipFile *zip.File) (*csv.File, error) {
	content, err := zipFile.Open()
	if err != nil {
		return nil, err
	}
	f, err := csv.New(file, content)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func parseAgencies(csv *csv.File) ([]Agency, []warnings.StaticWarning) {
	var w []warnings.StaticWarning
	idColumn := csv.OptionalColumn("agency_id")
	nameColumn := csv.RequiredColumn("agency_name")
	urlColumn := csv.RequiredColumn("agency_url")
	timezoneColumn := csv.RequiredColumn("agency_timezone")
	languageColumn := csv.OptionalColumn("agency_lang")
	phoneColumn := csv.OptionalColumn("agency_phone")
	fareUrlColumn := csv.OptionalColumn("agency_fare_url")
	emailColumn := csv.OptionalColumn("agency_email")

	if warnings := checkForMissingColumns(csv); len(warnings) > 0 {
		return nil, warnings
	}

	var agencies []Agency
	for csv.NextRow() {
		name := nameColumn.Read()
		agency := Agency{
			// TODO: support specifying the default agency ID in the GTFS static parser settings
			Id:       idColumn.ReadOr(fmt.Sprintf("%s_id", name)),
			Name:     name,
			Url:      urlColumn.Read(),
			Timezone: timezoneColumn.Read(),
			Language: languageColumn.Read(),
			Phone:    phoneColumn.Read(),
			FareUrl:  fareUrlColumn.Read(),
			Email:    emailColumn.Read(),
		}
		if missingKeys := csv.MissingRowKeys(); len(missingKeys) > 0 {
			w = append(w, warnings.NewStaticWarning(csv, warnings.AgencyMissingValues{
				AgencyID: agency.Id,
				Columns:  missingKeys,
			}))
			continue
		}
		agencies = append(agencies, agency)
	}
	return agencies, w
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
		if agencyID != "" {
			for i := range agencies {
				if agencies[i].Id == agencyID {
					agency = &agencies[i]
					break
				}
			}
			if agency == nil {
				log.Printf("skipping route %s: no match for agency ID %s", routeID, agencyID)
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
			Type:              parseRouteType_GTFSStatic(routeTypeColumn.Read()),
			Url:               urlColumn.Read(),
			SortOrder:         parseRouteSortOrder(sortOrderColumn.Read()),
			ContinuousPickup:  parsePickupDropOffPolicy(continuousPickupColumn.ReadOr("")),
			ContinuousDropOff: parsePickupDropOffPolicy(continuousDropOffColumn.ReadOr("")),
		}
		if missingKeys := csv.MissingRowKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping route %+v because of missing keys %s", route, missingKeys)
			continue
		}
		routes = append(routes, route)
	}
	return routes
}

func parseRouteSortOrder(raw string) *int32 {
	if raw == "" {
		return nil
	}
	i, err := strconv.Atoi(raw)
	if err != nil {
		return nil
	}
	i32 := int32(i)
	return &i32
}

func parseStops(csv *csv.File, inheritWheelchairBoarding bool) []Stop {
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
		stopID := idColumn.Read()
		hasParentStop := false
		if parentStopId := parentStationColumn.Read(); parentStopId != "" {
			stopIdToParent[stopID] = parentStopId
			hasParentStop = true
		}
		stop := Stop{
			Id:                 stopID,
			Code:               codeColumn.Read(),
			Name:               nameColumn.Read(),
			Description:        descriptionColumn.Read(),
			ZoneId:             zoneIdColumn.Read(),
			Longitude:          parseFloat64(longitudeColumn.Read()),
			Latitude:           parseFloat64(latitudeColumn.Read()),
			Url:                urlColumn.Read(),
			Type:               parseStopType(typeColumn.Read(), hasParentStop),
			Timezone:           timezoneColumn.Read(),
			WheelchairBoarding: parseWheelchairBoarding(wheelchairBoardingColumn.Read()),
			PlatformCode:       platformCodeColumn.Read(),
		}
		if missingKeys := csv.MissingRowKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping stop %+v because of missing keys %s", stop, missingKeys)
			continue
		}
		stopIdToIndex[stop.Id] = len(stops)
		stops = append(stops, stop)
	}
	for stopId, parentStopId := range stopIdToParent {
		parentStopIndex, ok := stopIdToIndex[parentStopId]
		if !ok {
			continue
		}
		stops[stopIdToIndex[stopId]].Parent = &stops[parentStopIndex]
	}

	// Inherit wheelchair boarding from parent stops if specified.
	if inheritWheelchairBoarding {
		for i := range stops {
			stop := &stops[i]
			if stop.Parent != nil && stop.Parent.Type == StopType_Station && stop.WheelchairBoarding == WheelchairBoarding_NotSpecified {
				stop.WheelchairBoarding = stop.Parent.WheelchairBoarding
			}
		}
	}

	return stops
}

func parseFloat64(s string) *float64 {
	if s == "" {
		return nil
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return nil
	}
	return &f
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

func parseInt32(s string) *int32 {
	if s == "" {
		return nil
	}
	i, err := strconv.ParseInt(s, 10, 32)
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
			DirectionId:          parseDirectionID_GTFSStatic(directionIDColumn.ReadOr("")),
			BlockID:              blockIDColumn.Read(),
			WheelchairAccessible: parseWheelchairBoarding(wheelchairAccessibleColumn.Read()),
			BikesAllowed:         parseBikesAllowed(bikesAllowedColumn.ReadOr("")),
		}

		shapeIDOrNil := shapeIDColumn.Read()
		if shapeIDOrNil != "" {
			if shape, ok := shapeIDToShape[shapeIDOrNil]; ok {
				trip.Shape = shape
			} else {
				log.Printf("Shape %s not found for trip %s", shapeIDOrNil, trip.ID)
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
			PickupType:            parsePickupDropOffPolicy(pickupTypeColumn.ReadOr("")),
			DropOffType:           parsePickupDropOffPolicy(dropOffTypeColumn.ReadOr("")),
			ContinuousPickup:      parsePickupDropOffPolicy(continuousPickupColumn.ReadOr("")),
			ContinuousDropOff:     parsePickupDropOffPolicy(continuousDropOffColumn.ReadOr("")),
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
		sort.Slice(trip.StopTimes, func(i, j int) bool {
			return trip.StopTimes[i].StopSequence < trip.StopTimes[j].StopSequence
		})
	}
}

func parseGtfsTimeToDuration(s string) (time.Duration, bool) {
	if s == "" {
		return 0, false
	}
	var pieces [3]int
	var i int
	for _, c := range s {
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
		shapePtLat := parseFloat64(shapePtLatColumn.Read())
		shapePtLon := parseFloat64(shapePtLonColumn.Read())
		shapePtSequence := parseInt32(shapePtSequenceColumn.Read())
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

		if missingKeys := csv.MissingRowKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping frequency because of missing keys %s", missingKeys)
			continue
		}
		scheduledTripOrNil := tripIDToScheduledTrip[tripID]
		if scheduledTripOrNil == nil {
			log.Printf("Skipping frequency because of missing trip %s", tripID)
			continue
		}
		headwaySecsOrNil := parseInt32(headwaySecs)
		if headwaySecsOrNil == nil {
			log.Print("Skipping frequency because of invalid headway_secs")
			continue
		}
		startTimeDuration, startTimeDurationOk := parseGtfsTimeToDuration(startTime)
		if !startTimeDurationOk {
			log.Print("Skipping frequency because of invalid start_time")
			continue
		}
		endTimeDuration, endTimeDurationOk := parseGtfsTimeToDuration(endTime)
		if !endTimeDurationOk {
			log.Print("Skipping frequency because of invalid end_time")
			continue
		}

		frequency := Frequency{
			StartTime:  startTimeDuration,
			EndTime:    endTimeDuration,
			Headway:    time.Duration(*headwaySecsOrNil) * time.Second,
			ExactTimes: parseExactTimes(exactTimesColumn.Read()),
		}

		scheduledTripOrNil.Frequencies = append(scheduledTripOrNil.Frequencies, frequency)
	}
}

func checkForMissingColumns(csv *csv.File) []warnings.StaticWarning {
	missing := csv.MissingRequiredColumns()
	if len(missing) == 0 {
		return nil
	}
	return []warnings.StaticWarning{
		warnings.NewStaticWarning(csv, warnings.MissingColumns{Columns: missing}),
	}
}
