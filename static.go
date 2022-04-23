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

	"github.com/jamespfennell/gtfs/csv"
)

// Static contains the parsed content for a single GTFS static message.
type Static struct {
	Agencies        []Agency
	Routes          []Route
	Stops           []Stop
	Transfers       []Transfer
	GroupedStations []Stop
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

type RoutePolicy int32

const (
	NotAllowed           RoutePolicy = 0
	Continuous           RoutePolicy = 1
	PhoneAgency          RoutePolicy = 2
	CoordinateWithDriver RoutePolicy = 3
)

func NewRoutePolicy(i int) RoutePolicy {
	var t RoutePolicy
	// TODO: figure out the mismatch here between 0 and 1
	switch i {
	case 0:
		t = Continuous
	case 1:
		t = NotAllowed
	case 2:
		t = PhoneAgency
	case 3:
		t = CoordinateWithDriver
	default:
		t = NotAllowed
	}
	return t
}

func (t RoutePolicy) String() string {
	switch t {
	case Continuous:
		return "ALLOWED"
	case PhoneAgency:
		return "PHONE_AGENCY"
	case CoordinateWithDriver:
		return "COORDINATE_WITH_DRIVER"
	case NotAllowed:
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
	ContinuousPickup  RoutePolicy
	ContinuousDropOff RoutePolicy
}

type Stop struct {
	Id                 string
	Code               *string
	Name               *string
	Description        *string
	ZoneId             *string
	Longitude          *float64
	Lattitude          *float64
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
	EntanceOrExit  StopType = 2
	GenericNode    StopType = 3
	BoardingArea   StopType = 4
	GroupedStation StopType = 1001
)

func NewStopType(i int) (StopType, bool) {
	var t StopType
	switch i {
	case 0:
		t = Platform
	case 1:
		t = Station
	case 2:
		t = EntanceOrExit
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
	case EntanceOrExit:
		return "ENTRANCE_OR_EXIT"
	case GenericNode:
		return "GENERIC_NODE"
	case BoardingArea:
		return "BOARDING_AREA"
	case GroupedStation:
		return "GROUPED_STATION"
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

type ParseStaticOptions struct {
	TransfersOptions TransfersOptions
}

type TransfersOptions struct {
	Strategy   TransfersStrategy
	Exceptions []struct {
		FromStopId string
		ToStopId   string
		Strategy   TransfersStrategy
	}
}

func (opts TransfersOptions) FindStrategy(fromStopId, toStopId string) TransfersStrategy {
	for _, exception := range opts.Exceptions {
		if fromStopId == exception.FromStopId && toStopId == exception.ToStopId {
			return exception.Strategy
		}
	}
	return opts.Strategy
}

type TransfersStrategy int32

const (
	TransfersStrategyDefault = 0
	GroupStations            = 1
)

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
	for _, table := range []struct {
		fileName string
		action   func(file *csv.File)
	}{
		{
			"agency.txt",
			func(file *csv.File) {
				result.Agencies = parseAgencies(file)
			},
		},
		{
			"routes.txt",
			func(file *csv.File) {
				result.Routes = parseRoutes(file, result.Agencies)
			},
		},
		{
			"stops.txt",
			func(file *csv.File) {
				result.Stops = parseStops(file)
			},
		},
		{
			"transfers.txt",
			func(file *csv.File) {
				result.Transfers, result.GroupedStations = parseTransfers(file, opts.TransfersOptions, result.Stops)
			},
		},
	} {
		file, err := readCsvFile(fileNameToFile, table.fileName)
		if err != nil {
			return nil, err
		}
		table.action(file)
	}
	return result, nil
}

func readCsvFile(fileNameToFile map[string]*zip.File, fileName string) (*csv.File, error) {
	zipFile := fileNameToFile[fileName]
	if zipFile == nil {
		return nil, fmt.Errorf("no %q file in GTFS static feed", fileName)
	}
	content, err := zipFile.Open()
	if err != nil {
		return nil, err
	}
	defer content.Close()
	f, err := csv.New(content)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %q: %w", fileName, err)
	}
	return f, nil
}

func parseAgencies(csv *csv.File) []Agency {
	var agencies []Agency
	iter := csv.Iter()
	for iter.Next() {
		row := iter.Get()
		agency := Agency{
			Id: row.GetOrCalculate("agency_id", func() string {
				// TODO: support specifying the agency ID in the GTFS static parser settings
				return fmt.Sprintf("%s_id", row.Get("agency_name"))
			}),
			Name:     row.Get("agency_name"),
			Url:      row.Get("agency_url"),
			Timezone: row.Get("agency_timezone"),
			Language: row.GetOptional("agency_lang"),
			Phone:    row.GetOptional("agency_phone"),
			FareUrl:  row.GetOptional("agency_fare_url"),
			Email:    row.GetOptional("agency_email"),
		}
		if missingKeys := row.MissingKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping agency %+v because of missing keys %s", agency, missingKeys)
			continue
		}
		agencies = append(agencies, agency)
	}
	return agencies
}

func parseRoutes(csv *csv.File, agencies []Agency) []Route {
	var routes []Route
	iter := csv.Iter()
	for iter.Next() {
		row := iter.Get()
		agencyId := row.GetOptional("agency_id")
		var agency *Agency
		if agencyId != nil {
			for i := range agencies {
				if agencies[i].Id == *agencyId {
					agency = &agencies[i]
					break
				}
			}
			if agency == nil {
				log.Printf("skipping route %s: no match for agency ID %s", row.Get("route_id"), *agencyId)
				continue
			}
		} else if len(agencies) == 1 {
			// In GTFS static if there is a single agency, a route's agency ID field can be omitted in
			// which case the route's agency is the unique agency in the feed.
			agency = &agencies[0]
		} else {
			log.Printf("skipping route %s: no agency ID provided but no unique agency", row.Get("route_id"))
			continue
		}
		route := Route{
			Id:                row.Get("route_id"),
			Agency:            agency,
			Color:             row.GetOr("route_color", "FFFFFF"),
			TextColor:         row.GetOr("route_text_color", "000000"),
			ShortName:         row.GetOptional("route_short_name"),
			LongName:          row.GetOptional("route_long_name"),
			Description:       row.GetOptional("route_desc"),
			Type:              parseRouteType(row.Get("route_type")),
			Url:               row.GetOptional("route_url"),
			SortOrder:         parseRouteSortOrder(row.GetOptional("route_sort_order")),
			ContinuousPickup:  parseRoutePolicy(row.GetOptional("continuous_pickup")),
			ContinuousDropOff: parseRoutePolicy(row.GetOptional("continuous_dropoff")),
		}
		if missingKeys := row.MissingKeys(); len(missingKeys) > 0 {
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

func parseRoutePolicy(raw *string) RoutePolicy {
	if raw == nil {
		return NotAllowed
	}
	i, err := strconv.Atoi(*raw)
	if err != nil {
		return NotAllowed
	}
	return NewRoutePolicy(i)
}

func parseStops(csv *csv.File) []Stop {
	var stops []Stop
	stopIdToIndex := map[string]int{}
	stopIdToParent := map[string]string{}
	iter := csv.Iter()
	for iter.Next() {
		row := iter.Get()
		stop := Stop{
			Id:                 row.Get("stop_id"),
			Code:               row.GetOptional("stop_code"),
			Name:               row.GetOptional("stop_name"),
			Description:        row.GetOptional("stop_desc"),
			ZoneId:             row.GetOptional("zone_id"),
			Longitude:          parseFloat64(row.GetOptional("stop_lon")),
			Lattitude:          parseFloat64(row.GetOptional("stop_lat")),
			Url:                row.GetOptional("stop_url"),
			Type:               parseStopType(row.GetOptional("location_type")),
			Timezone:           row.GetOptional("stop_timezone"),
			WheelchairBoarding: parseWheelchairBoarding(row.GetOptional("wheelchair_boarding")),
			PlatformCode:       row.GetOptional("platform_code"),
		}
		if missingKeys := row.MissingKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping stop %+v because of missing keys %s", stop, missingKeys)
			continue
		}
		stopIdToIndex[stop.Id] = len(stops)
		parentStopId := row.GetOptional("parent_station")
		if parentStopId != nil {
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
	f, err := strconv.ParseFloat(*raw, 64)
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

func parseTransfers(csv *csv.File, opts TransfersOptions, stops []Stop) ([]Transfer, []Stop) {
	stopIdToStop := map[string]*Stop{}
	for i := range stops {
		stopIdToStop[stops[i].Id] = &stops[i]
	}
	groupingMap := map[string]map[string]bool{}
	var transfers []Transfer
	iter := csv.Iter()
	for iter.Next() {
		row := iter.Get()
		fromStop, fromStopOk := stopIdToStop[row.Get("from_stop_id")]
		toStop, toStopOk := stopIdToStop[row.Get("to_stop_id")]
		if missingKeys := row.MissingKeys(); len(missingKeys) > 0 {
			log.Printf("Skipping transfer because of missing keys %s", missingKeys)
			continue
		}
		if !fromStopOk {
			log.Printf("Skipping transfer because from_stop_id %q is invalid", row.Get("from_stop_id"))
			continue
		}
		if !toStopOk {
			log.Printf("Skipping transfer because to_stop_id %q is invalid", row.Get("to_stop_id"))
			continue
		}
		if fromStop.Id == toStop.Id {
			log.Printf("Skipping transfer between the same stop %q", fromStop.Id)
			continue
		}
		switch opts.FindStrategy(fromStop.Id, toStop.Id) {
		case GroupStations:
			updateGroupingMap(groupingMap, fromStop.Root().Id, toStop.Root().Id)
		default:
			transfer := Transfer{
				From:            fromStop,
				To:              toStop,
				Type:            parseTransferType(row.GetOptional("transfer_type")),
				MinTransferTime: parseInt32(row.GetOptional("min_transfer_time")),
			}
			transfers = append(transfers, transfer)
		}
	}
	return transfers, buildGroupedStations(stopIdToStop, groupingMap)
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

func updateGroupingMap(m map[string]map[string]bool, stopId1, stopId2 string) {
	allStops := map[string]bool{
		stopId1: true,
		stopId2: true,
	}
	for stopId := range m[stopId1] {
		allStops[stopId] = true
	}
	for stopId := range m[stopId2] {
		allStops[stopId] = true
	}
	for stopId := range allStops {
		m[stopId] = allStops
	}
}

func buildGroupedStations(existingStops map[string]*Stop, groupingMap map[string]map[string]bool) []Stop {
	var newStops []Stop
	var children []map[string]bool
	newStopIds := map[string]bool{}
	for _, stopIdsSet := range groupingMap {
		var stopIds []string
		for stopId := range stopIdsSet {
			stopIds = append(stopIds, stopId)
		}
		sort.Strings(stopIds)
		newStopId := strings.Join(stopIds, "-")
		if _, alreadyCreated := newStopIds[newStopId]; alreadyCreated {
			continue
		}
		newStopIds[newStopId] = true
		children = append(children, stopIdsSet)
		newStops = append(newStops, Stop{
			Id:   newStopId,
			Type: GroupedStation,
		})
	}
	for i, childStopIds := range children {
		for childStopId := range childStopIds {
			existingStops[childStopId].Parent = &newStops[i]
		}
	}
	return newStops
}
