// Package journal contains a tool for building trip journals.
package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jamespfennell/gtfs"
	"github.com/jamespfennell/gtfs/extensions/nycttrips"
)

type Journal struct {
	Trips []Trip
	// TODO: Metadata
	// TODO: filter on start_time
	// TODO: log an error if the trip passes the filter but was updated in the last update
	//if trip.NumScheduleChanges > 0 {
	//	a += 1
	//}
}

type Trip struct {
	TripUID     string
	TripID      string
	RouteID     string
	DirectionID gtfs.DirectionID
	StartTime   time.Time
	VehicleID   string
	IsAssigned  bool

	StopTimes []StopTime

	// Metadata follows
	LastObserved        time.Time
	MarkedPast          *time.Time
	NumUpdates          int
	NumScheduleChanges  int
	NumScheduleRewrites int
}

type StopTime struct {
	StopID        string
	ArrivalTime   *time.Time
	DepartureTime *time.Time
	Track         *string

	LastObserved time.Time
	MarkedPast   *time.Time
}

// TODO: rename Source
type GtfsrtSource interface {
	// TODO: change this to return (*gtfs.RealTime, error) and add logic in BuildJournal to be resilient to this
	Next() *gtfs.Realtime
}

type DirectoryGtfsrtSource struct {
	baseDir   string
	fileNames []string
	startLen  int
	t         time.Time
}

func NewDirectoryGtfsrtSource(baseDir string) (*DirectoryGtfsrtSource, error) {
	files, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}
	source := &DirectoryGtfsrtSource{
		baseDir: baseDir,
		t:       time.Now(),
	}
	for _, file := range files {
		source.fileNames = append(source.fileNames, file.Name())
	}
	source.startLen = len(source.fileNames)
	sort.Strings(source.fileNames)
	return source, nil
}

func (s *DirectoryGtfsrtSource) Next() *gtfs.Realtime {
	for {
		if len(s.fileNames) == 0 {
			return nil
		}
		filePath := filepath.Join(s.baseDir, s.fileNames[0])
		s.fileNames = s.fileNames[1:]
		b, err := os.ReadFile(filePath)
		if err != nil {
			// TODO: debug logging
			// log.Printf("Failed to read %s: %s", filePath, err)
			continue
		}
		extension := nycttrips.Extension(nycttrips.ExtensionOpts{
			FilterStaleUnassignedTrips:        true,
			PreserveMTrainPlatformsInBushwick: false,
		})
		result, err := gtfs.ParseRealtime(b, &gtfs.ParseRealtimeOptions{
			Extension: extension,
		})
		if err != nil {
			// TODO: debug logging
			// log.Printf("Failed to parse %s as a GTFS Realtime message: %s", filePath, err)
			continue
		}
		if time.Since(s.t) >= time.Second {
			// TODO: debug logging
			// log.Printf("Processed %d/%d files\n", s.startLen-len(s.fileNames), s.startLen)
			s.t = time.Now()
		}
		return result
	}
}

func BuildJournal(source GtfsrtSource, startTime, endTime time.Time) *Journal {
	trips := map[string]*Trip{}
	activeTrips := map[string]bool{}
	i := 0
	for feedMessage := source.Next(); feedMessage != nil; feedMessage = source.Next() {
		feedMessage := feedMessage
		createdAt := feedMessage.CreatedAt
		newActiveTrips := map[string]bool{}
		for _, tripUpdate := range feedMessage.Trips {
			startTime := tripUpdate.ID.StartDate.Add(tripUpdate.ID.StartTime)
			tripUID := fmt.Sprintf("%d%s", startTime.Unix(), tripUpdate.ID.ID[6:])
			if existingTrip, ok := trips[tripUID]; ok {
				existingTrip.update(&tripUpdate, createdAt)
			} else {
				trip := Trip{
					// One rewrite+change is expected at the start
					NumScheduleChanges:  -1,
					NumScheduleRewrites: -1,
				}
				trip.update(&tripUpdate, createdAt)
				trips[tripUID] = &trip
			}
			newActiveTrips[tripUID] = true
		}
		for tripUID := range activeTrips {
			if newActiveTrips[tripUID] {
				continue
			}
			trips[tripUID].markPast(createdAt)
		}
		activeTrips = newActiveTrips
		i++
	}
	var tripIDs []string
	for tripID, trip := range trips {
		if trip.StartTime.Before(startTime) || endTime.Before(trip.StartTime) {
			continue
		}
		if !trip.IsAssigned {
			// TODO: debug logging
			// log.Printf("Skipping return of unassigned trip %s\n", trip.TripUID)
			continue
		}
		tripIDs = append(tripIDs, tripID)
	}
	sort.Strings(tripIDs)
	j := &Journal{}
	for _, tripID := range tripIDs {
		j.Trips = append(j.Trips, *trips[tripID])
	}
	return j
}

func (trip *Trip) update(tripUpdate *gtfs.Trip, feedCreatedAt time.Time) {
	if trip.IsAssigned && tripUpdate.Vehicle == nil {
		// TODO: this seems to happen a lot, would be nice to figure out what's happening.
		// log.Printf("skipping unassigned update for assigned trip %s\n", trip.TripUID)
		return
	}
	startTime := tripUpdate.ID.StartDate.Add(tripUpdate.ID.StartTime)
	vehicle := tripUpdate.GetVehicle()

	trip.TripUID = fmt.Sprintf("%d%s", startTime.Unix(), tripUpdate.ID.ID[6:])
	trip.TripID = tripUpdate.ID.ID
	trip.RouteID = tripUpdate.ID.RouteID
	trip.DirectionID = tripUpdate.ID.DirectionID
	trip.StartTime = tripUpdate.ID.StartDate.Add(tripUpdate.ID.StartTime)
	trip.VehicleID = vehicle.GetID().ID
	trip.IsAssigned = trip.IsAssigned || tripUpdate.Vehicle != nil

	trip.LastObserved = feedCreatedAt
	trip.MarkedPast = nil
	trip.NumUpdates += 1

	stopTimeUpdates := tripUpdate.StopTimeUpdates

	p := createPartition(trip.StopTimes, stopTimeUpdates)

	// Mark old stop times as past
	for i := range p.past {
		p.past[i].markPast(feedCreatedAt)
	}

	// Update existing stop times
	for _, update := range p.updated {
		update.existing.update(update.update, feedCreatedAt)
	}

	// Trim obsolete stop times (e.g. from a schedule change)
	trip.StopTimes = trip.StopTimes[:len(p.past)+len(p.updated)]
	if len(trip.StopTimes) == 0 {
		trip.NumScheduleRewrites += 1
	}

	// Add new stop times
	for i := range p.new {
		stopTime := StopTime{}
		stopTime.update(&p.new[i], feedCreatedAt)
		trip.StopTimes = append(trip.StopTimes, stopTime)
	}
	if len(p.new) != 0 {
		trip.NumScheduleChanges += 1
	}
}

func (trip *Trip) markPast(feedCreatedAt time.Time) {
	if trip.MarkedPast == nil {
		trip.MarkedPast = &feedCreatedAt
	}
	for i := 0; i < len(trip.StopTimes); i++ {
		trip.StopTimes[i].markPast(feedCreatedAt)
	}
}

type updated struct {
	existing *StopTime
	update   *gtfs.StopTimeUpdate
}

type partition struct {
	past    []StopTime
	updated []updated
	new     []gtfs.StopTimeUpdate
}

func createPartition(stopTimes []StopTime, updates []gtfs.StopTimeUpdate) partition {
	if len(updates) == 0 {
		return partition{
			past: stopTimes,
		}
	}
	var p partition

	firstUpdatedStopID := *updates[0].StopID
	firstUpdatedStopTimeIndex := 0
	for i, stopTime := range stopTimes {
		if stopTime.StopID == firstUpdatedStopID {
			firstUpdatedStopTimeIndex = i
			break
		}
	}
	p.past = stopTimes[:firstUpdatedStopTimeIndex]

	updateIndex := 0
	for i := range stopTimes[firstUpdatedStopTimeIndex:] {
		if updateIndex >= len(updates) {
			break
		}
		stopTime := &stopTimes[firstUpdatedStopTimeIndex+i]
		update := &updates[updateIndex]
		if stopTime.StopID != *update.StopID {
			break
		}
		p.updated = append(p.updated, updated{
			existing: stopTime,
			update:   update,
		})
		updateIndex += 1
	}

	p.new = updates[updateIndex:]

	return p
}

func (stopTime *StopTime) update(stopTimeUpdate *gtfs.StopTimeUpdate, feedCreatedAt time.Time) {
	stopTime.StopID = *stopTimeUpdate.StopID
	stopTime.ArrivalTime = stopTimeUpdate.GetArrival().Time
	stopTime.DepartureTime = stopTimeUpdate.GetDeparture().Time
	stopTime.Track = stopTimeUpdate.NyctTrack
	stopTime.LastObserved = feedCreatedAt
	stopTime.MarkedPast = nil
}

func (stopTime *StopTime) markPast(feedCreatedAt time.Time) {
	if stopTime.MarkedPast == nil {
		stopTime.MarkedPast = &feedCreatedAt
	}
}
