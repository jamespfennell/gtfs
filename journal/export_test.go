package journal

import (
	"testing"
	"time"

	"github.com/jamespfennell/gtfs"
)

var trip Trip = Trip{
	TripUID:     "TripUID",
	TripID:      "TripID",
	RouteID:     "RouteID",
	DirectionID: gtfs.DirectionID_True,
	VehicleID:   "VehicleID",
	StartTime:   time.Unix(100, 0),
	StopTimes: []StopTime{
		{
			StopID:        "StopID1",
			Track:         ptr("Track1"),
			ArrivalTime:   nil,
			DepartureTime: ptr(time.Unix(200, 0)),
			LastObserved:  time.Unix(200, 0),
			MarkedPast:    ptr(time.Unix(300, 0)),
		},
		{
			StopID:        "StopID2",
			ArrivalTime:   ptr(time.Unix(300, 0)),
			DepartureTime: ptr(time.Unix(400, 0)),
			LastObserved:  time.Unix(400, 0),
		},
		{
			StopID:        "StopID3",
			Track:         ptr("Track3"),
			ArrivalTime:   ptr(time.Unix(500, 0)),
			DepartureTime: nil,
			LastObserved:  time.Unix(400, 0),
		},
	},
	LastObserved:        time.Unix(400, 0),
	MarkedPast:          ptr(time.Unix(600, 0)),
	NumUpdates:          100,
	NumScheduleChanges:  2,
	NumScheduleRewrites: 1,
}

const expectedTripsCsv = `trip_uid,trip_id,route_id,direction_id,start_time,vehicle_id,last_observed,marked_past,num_updates,num_schedule_changes,num_schedule_rewrites
TripUID,TripID,RouteID,1,100,VehicleID,400,600,100,2,1
`

const expectedStopTimesCsv = `trip_uid,stop_id,track,arrival_time,departure_time,last_observed,marked_past
TripUID,StopID1,Track1,,200,200,300
TripUID,StopID2,,300,400,400,
TripUID,StopID3,Track3,500,,400,
`

func TestCsvExport(t *testing.T) {
	journal := Journal{Trips: []Trip{trip}}

	result, err := journal.ExportToCsv()
	if err != nil {
		t.Fatalf("AsCsv function failed: %s", err)
	}

	if got, want := string(result.TripsCsv), expectedTripsCsv; got != want {
		t.Errorf("Trips file actual:\n%s\n!= expected:\n%s\n", got, want)
	}

	if got, want := string(result.StopTimesCsv), expectedStopTimesCsv; got != want {
		t.Errorf("Stop times file actual:\n%s\n!= expected:\n%s\n", got, want)
	}
}
