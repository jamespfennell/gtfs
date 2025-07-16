package gtfs

import (
	"math"
	"testing"
	"time"
)

// Simple helper: parse "08:00:00" to time.Duration
func dur(s string) time.Duration {
	t, _ := time.Parse("15:04:05", s)
	return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second
}
func almostEq(a, b time.Duration) bool {
	return math.Abs(float64(a-b)) < float64(time.Second)
}

// --- Tests for interpolateStopTimes (equal, no distance) ---

func TestInterpolateStopTimes_Normal(t *testing.T) {
	st := []ScheduledStopTime{
		{StopSequence: 1, ArrivalTime: dur("08:00:00"), DepartureTime: dur("08:05:00"), ExactTimes: true},
		{StopSequence: 2, ArrivalTime: 0, DepartureTime: 0},
		{StopSequence: 3, ArrivalTime: 0, DepartureTime: 0},
		{StopSequence: 4, ArrivalTime: dur("08:30:00"), DepartureTime: dur("08:35:00"), ExactTimes: true},
	}
	wantArr := []time.Duration{dur("08:00:00"), dur("08:10:00"), dur("08:20:00"), dur("08:30:00")}
	wantDep := []time.Duration{dur("08:05:00"), dur("08:15:00"), dur("08:25:00"), dur("08:35:00")}
	wantTimePoint := []bool{true, false, false, true}

	got := interpolateStopTimes(st)
	for i := range got {
		if !almostEq(got[i].ArrivalTime, wantArr[i]) {
			t.Errorf("normal: arrival %d: want %v got %v", i, wantArr[i], got[i].ArrivalTime)
		}
		if !almostEq(got[i].DepartureTime, wantDep[i]) {
			t.Errorf("normal: depart %d: want %v got %v", i, wantDep[i], got[i].DepartureTime)
		}
		if got[i].ExactTimes != wantTimePoint[i] {
			t.Errorf("timepoint %d: want %v got %v", i, wantTimePoint[i], got[i].ExactTimes)
		}
	}
}

func TestInterpolateStopTimes_MissingFirst(t *testing.T) {
	st := []ScheduledStopTime{
		{StopSequence: 1, ArrivalTime: 0, DepartureTime: 0},
		{StopSequence: 2, ArrivalTime: 0, DepartureTime: 0},
		{StopSequence: 3, ArrivalTime: dur("08:30:00"), DepartureTime: dur("08:35:00")},
	}
	got := interpolateStopTimes(st)
	// The first two should remain zero, last should be as input
	if got[0].ArrivalTime != 0 || got[1].ArrivalTime != 0 {
		t.Errorf("first missing: arrivals should remain zero, got: %v %v", got[0].ArrivalTime, got[1].ArrivalTime)
	}
	if got[0].DepartureTime != 0 || got[1].DepartureTime != 0 {
		t.Errorf("first missing: departures should remain zero, got: %v %v", got[0].DepartureTime, got[1].DepartureTime)
	}
	if !almostEq(got[2].ArrivalTime, dur("08:30:00")) {
		t.Errorf("first missing: last arrival wrong, got %v", got[2].ArrivalTime)
	}
	if !almostEq(got[2].DepartureTime, dur("08:35:00")) {
		t.Errorf("first missing: last depart wrong, got %v", got[2].DepartureTime)
	}
}

func TestInterpolateStopTimes_MissingLast(t *testing.T) {
	st := []ScheduledStopTime{
		{StopSequence: 1, ArrivalTime: dur("08:00:00"), DepartureTime: dur("08:05:00")},
		{StopSequence: 2, ArrivalTime: 0, DepartureTime: 0},
		{StopSequence: 3, ArrivalTime: 0, DepartureTime: 0},
	}
	got := interpolateStopTimes(st)
	// The first should be as input, the last two should remain zero
	if !almostEq(got[0].ArrivalTime, dur("08:00:00")) {
		t.Errorf("last missing: first arrival wrong, got %v", got[0].ArrivalTime)
	}
	if !almostEq(got[0].DepartureTime, dur("08:05:00")) {
		t.Errorf("last missing: first depart wrong, got %v", got[0].DepartureTime)
	}
	if got[1].ArrivalTime != 0 || got[2].ArrivalTime != 0 {
		t.Errorf("last missing: arrivals should remain zero, got: %v %v", got[1].ArrivalTime, got[2].ArrivalTime)
	}
	if got[1].DepartureTime != 0 || got[2].DepartureTime != 0 {
		t.Errorf("last missing: departures should remain zero, got: %v %v", got[1].DepartureTime, got[2].DepartureTime)
	}
}

// --- Tests for interpolateStopTimesByShapeDist (distance-weighted) ---

func TestInterpolateStopTimesByShapeDist_Normal(t *testing.T) {
	st := []ScheduledStopTime{
		{StopSequence: 1, ArrivalTime: dur("08:00:00"), DepartureTime: dur("08:05:00"), ShapeDistanceTraveled: ptr(0.0), ExactTimes: true},
		{StopSequence: 2, ArrivalTime: 0, DepartureTime: 0, ShapeDistanceTraveled: ptr(3.5)},
		{StopSequence: 3, ArrivalTime: 0, DepartureTime: 0, ShapeDistanceTraveled: ptr(7.0)},
		{StopSequence: 4, ArrivalTime: dur("08:30:00"), DepartureTime: dur("08:35:00"), ShapeDistanceTraveled: ptr(10.5), ExactTimes: true},
	}
	wantArr := []time.Duration{dur("08:00:00"), dur("08:10:00"), dur("08:20:00"), dur("08:30:00")}
	wantDep := []time.Duration{dur("08:05:00"), dur("08:15:00"), dur("08:25:00"), dur("08:35:00")}
	wantTimePoint := []bool{true, false, false, true}

	got := interpolateStopTimesByShapeDist(st)
	for i := range got {
		if !almostEq(got[i].ArrivalTime, wantArr[i]) {
			t.Errorf("shape normal: arrival %d: want %v got %v", i, wantArr[i], got[i].ArrivalTime)
		}
		if !almostEq(got[i].DepartureTime, wantDep[i]) {
			t.Errorf("shape normal: depart %d: want %v got %v", i, wantDep[i], got[i].DepartureTime)
		}
		if got[i].ExactTimes != wantTimePoint[i] {
			t.Errorf("timepoint %d: want %v got %v", i, wantTimePoint[i], got[i].ExactTimes)
		}
	}
}

func TestInterpolateStopTimesByShapeDist_MissingFirst(t *testing.T) {
	st := []ScheduledStopTime{
		{StopSequence: 1, ArrivalTime: 0, DepartureTime: 0, ShapeDistanceTraveled: ptr(0.0)},
		{StopSequence: 2, ArrivalTime: 0, DepartureTime: 0, ShapeDistanceTraveled: ptr(3.5)},
		{StopSequence: 3, ArrivalTime: dur("08:30:00"), DepartureTime: dur("08:35:00"), ShapeDistanceTraveled: ptr(10.5)},
	}
	got := interpolateStopTimesByShapeDist(st)
	// The first two should remain zero, last should be as input
	if got[0].ArrivalTime != 0 || got[1].ArrivalTime != 0 {
		t.Errorf("shape first missing: arrivals should remain zero, got: %v %v", got[0].ArrivalTime, got[1].ArrivalTime)
	}
	if got[0].DepartureTime != 0 || got[1].DepartureTime != 0 {
		t.Errorf("shape first missing: departures should remain zero, got: %v %v", got[0].DepartureTime, got[1].DepartureTime)
	}
	if !almostEq(got[2].ArrivalTime, dur("08:30:00")) {
		t.Errorf("shape first missing: last arrival wrong, got %v", got[2].ArrivalTime)
	}
	if !almostEq(got[2].DepartureTime, dur("08:35:00")) {
		t.Errorf("shape first missing: last depart wrong, got %v", got[2].DepartureTime)
	}
}

func TestInterpolateStopTimesByShapeDist_MissingLast(t *testing.T) {
	st := []ScheduledStopTime{
		{StopSequence: 1, ArrivalTime: dur("08:00:00"), DepartureTime: dur("08:05:00"), ShapeDistanceTraveled: ptr(0.0)},
		{StopSequence: 2, ArrivalTime: 0, DepartureTime: 0, ShapeDistanceTraveled: ptr(3.5)},
		{StopSequence: 3, ArrivalTime: 0, DepartureTime: 0, ShapeDistanceTraveled: ptr(10.5)},
	}
	got := interpolateStopTimesByShapeDist(st)
	// The first should be as input, the last two should remain zero
	if !almostEq(got[0].ArrivalTime, dur("08:00:00")) {
		t.Errorf("shape last missing: first arrival wrong, got %v", got[0].ArrivalTime)
	}
	if !almostEq(got[0].DepartureTime, dur("08:05:00")) {
		t.Errorf("shape last missing: first depart wrong, got %v", got[0].DepartureTime)
	}
	if got[1].ArrivalTime != 0 || got[2].ArrivalTime != 0 {
		t.Errorf("shape last missing: arrivals should remain zero, got: %v %v", got[1].ArrivalTime, got[2].ArrivalTime)
	}
	if got[1].DepartureTime != 0 || got[2].DepartureTime != 0 {
		t.Errorf("shape last missing: departures should remain zero, got: %v %v", got[1].DepartureTime, got[2].DepartureTime)
	}
}
