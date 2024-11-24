package journal

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jamespfennell/gtfs"
)

const tripID1 = "123456_L_1"
const routeID1 = "routeID1"
const trainID1 = "trainID1"
const stopID1 = "stopID1"
const stopID2 = "stopID2"
const stopID3 = "stopID3"
const stopID4 = "stopID4"
const stopID5 = "stopID5"

func TestJournal(t *testing.T) {
	gtfsTrip := gtfs.Trip{
		ID: gtfs.TripID{
			ID:          tripID1,
			RouteID:     routeID1,
			DirectionID: gtfs.DirectionID_True,
			StartTime:   100 * time.Second,
			StartDate:   mt(0),
		},
		Vehicle: &gtfs.Vehicle{
			ID: &gtfs.VehicleID{
				ID: trainID1,
			},
		},
	}
	source := &testGtfsrtSource{
		feeds: []*gtfs.Realtime{
			{
				CreatedAt: mt(0),
				Trips: []gtfs.Trip{
					addStopTimes(
						gtfsTrip,
						gtfs.StopTimeUpdate{
							StopID: ptr(stopID1),
							Departure: &gtfs.StopTimeEvent{
								Time: mtp(0),
							},
						},
						gtfs.StopTimeUpdate{
							StopID: ptr(stopID2),
							Arrival: &gtfs.StopTimeEvent{
								Time: mtp(8),
							},
							Departure: &gtfs.StopTimeEvent{
								Time: mtp(10),
							},
						},
						gtfs.StopTimeUpdate{
							StopID: ptr(stopID4),
							Arrival: &gtfs.StopTimeEvent{
								Time: mtp(20),
							},
							Departure: &gtfs.StopTimeEvent{
								Time: mtp(22),
							},
						},
						gtfs.StopTimeUpdate{
							StopID: ptr(stopID5),
							Arrival: &gtfs.StopTimeEvent{
								Time: mtp(20),
							},
						},
					),
				},
			},
			{
				CreatedAt: mt(7),
				Trips: []gtfs.Trip{
					addStopTimes(
						gtfsTrip,
						gtfs.StopTimeUpdate{
							StopID: ptr(stopID2),
							Arrival: &gtfs.StopTimeEvent{
								Time: mtp(9),
							},
							Departure: &gtfs.StopTimeEvent{
								Time: mtp(11),
							},
						},
						gtfs.StopTimeUpdate{
							StopID: ptr(stopID3),
							Arrival: &gtfs.StopTimeEvent{
								Time: mtp(21),
							},
						},
					),
				},
			},
			{
				CreatedAt: mt(14),
				Trips: []gtfs.Trip{
					addStopTimes(
						gtfsTrip,
						gtfs.StopTimeUpdate{
							StopID: ptr(stopID3),
							Arrival: &gtfs.StopTimeEvent{
								Time: mtp(20),
							},
						},
					),
				},
			},
			{
				CreatedAt: mt(21),
				Trips:     []gtfs.Trip{},
			},
		},
	}
	expected := Journal{
		Trips: []Trip{
			{
				TripUID:     "100_L_1",
				TripID:      tripID1,
				RouteID:     routeID1,
				DirectionID: gtfs.DirectionID_True,
				StartTime:   time.Unix(100, 0).UTC(),
				VehicleID:   trainID1,
				IsAssigned:  true,
				StopTimes: []StopTime{
					{
						StopID:        stopID1,
						DepartureTime: mtp(0),
						LastObserved:  mt(0),
						MarkedPast:    mtp(7),
					},
					{
						StopID:        stopID2,
						ArrivalTime:   mtp(9),
						DepartureTime: mtp(11),
						LastObserved:  mt(7),
						MarkedPast:    mtp(14),
					},
					{
						StopID:       stopID3,
						ArrivalTime:  mtp(20),
						LastObserved: mt(14),
						MarkedPast:   mtp(21),
					},
				},
				LastObserved:       mt(14),
				MarkedPast:         mtp(21),
				NumUpdates:         3,
				NumScheduleChanges: 1,
			},
		},
	}
	j := BuildJournal(source, time.Unix(0, 0), time.Unix(10000, 0))

	if diff := cmp.Diff(j, &expected); diff != "" {
		t.Errorf("Actual:\n%+v\n!= expected:\n%+v\ndiff:%s", j, &expected, diff)
	}
}

type testGtfsrtSource struct {
	feeds []*gtfs.Realtime
}

func (s *testGtfsrtSource) Next() *gtfs.Realtime {
	if len(s.feeds) == 0 {
		return nil
	}
	next := s.feeds[0]
	s.feeds = s.feeds[1:]
	return next
}

func addStopTimes(t gtfs.Trip, stopTimes ...gtfs.StopTimeUpdate) gtfs.Trip {
	t.StopTimeUpdates = stopTimes
	return t
}

func mt(r int64) time.Time {
	return time.Unix(600*r, 0).UTC()
}

func mtp(r int64) *time.Time {
	t := mt(r)
	return &t
}

func ptr(t string) *string {
	return &t
}
