package gtfs

import (
	"crypto/md5"
	"fmt"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
	"testing"
	"time"
)

func BenchmarkHashTrip(b *testing.B) {
	trip := mkTrip(0)
	for n := 0; n < b.N; n++ {
		h := md5.New()
		trip.Hash(h)
		_ = h.Sum(nil)
	}
}

func TestHashTrip(t *testing.T) {
	for _, tc := range []struct {
		field  string
		getter func(t *Trip) any
	}{
		{
			"id.id",
			func(t *Trip) any {
				return &t.ID.ID
			},
		},
		{
			"id.route_id",
			func(t *Trip) any {
				return &t.ID.RouteID
			},
		},
		{
			"id.has_start_date",
			func(t *Trip) any {
				return &t.ID.HasStartDate
			},
		},
		{
			"id.start_date",
			func(t *Trip) any {
				return &t.ID.StartDate
			},
		},
		{
			"id.has_start_time",
			func(t *Trip) any {
				return &t.ID.HasStartTime
			},
		},
		{
			"id.start_time",
			func(t *Trip) any {
				return &t.ID.StartTime
			},
		},
		{
			"nyct_is_assigned",
			func(t *Trip) any {
				return &t.NyctIsAssigned
			},
		},
		{
			"stop_time_updates[0].stop_sequence",
			func(t *Trip) any {
				return &t.StopTimeUpdates[0].StopSequence
			},
		},
		{
			"stop_time_updates[0].stop_id",
			func(t *Trip) any {
				return &t.StopTimeUpdates[0].StopID
			},
		},
		{
			"stop_time_updates[0].nyct_track",
			func(t *Trip) any {
				return &t.StopTimeUpdates[0].NyctTrack
			},
		},
		{
			"stop_time_updates[0].arrival",
			func(t *Trip) any {
				return &t.StopTimeUpdates[0].Arrival
			},
		},
		{
			"stop_time_updates[0].arrival.time",
			func(t *Trip) any {
				return &t.StopTimeUpdates[0].Arrival.Time
			},
		},
		{
			"stop_time_updates[0].arrival.delay",
			func(t *Trip) any {
				return &t.StopTimeUpdates[0].Arrival.Delay
			},
		},
		{
			"stop_time_updates[0].arrival.uncertainty",
			func(t *Trip) any {
				return &t.StopTimeUpdates[0].Arrival.Uncertainty
			},
		},
		{
			"stop_time_updates[0].departure",
			func(t *Trip) any {
				return &t.StopTimeUpdates[0].Departure
			},
		},
		{
			"stop_time_updates[0].departure.time",
			func(t *Trip) any {
				return &t.StopTimeUpdates[0].Departure.Time
			},
		},
		{
			"stop_time_updates[0].departure.delay",
			func(t *Trip) any {
				return &t.StopTimeUpdates[0].Departure.Delay
			},
		},
		{
			"stop_time_updates[0].departure.uncertainty",
			func(t *Trip) any {
				return &t.StopTimeUpdates[0].Departure.Uncertainty
			},
		},
	} {
		t.Run(tc.field, func(t *testing.T) {
			trip := mkTrip(0)
			modifierPairs := combinations(allModifiers(tc.getter(&trip)))
			for _, pair := range modifierPairs {
				t.Run(fmt.Sprintf("%s-%s", pair[0].name, pair[1].name), func(t *testing.T) {
					trip1 := mkTrip(0)
					pair[0].fn(tc.getter(&trip1))
					h1 := md5.New()
					trip1.Hash(h1)
					s1 := fmt.Sprintf("%x", h1.Sum(nil))

					trip2 := mkTrip(0)
					pair[1].fn(tc.getter(&trip2))
					h2 := md5.New()
					trip2.Hash(h2)
					s2 := fmt.Sprintf("%x", h2.Sum(nil))

					if s1 == s2 {
						t.Errorf("hashes match but trips are different\ntrip1: %v\ntrip2: %v", trip1, trip2)
					}
				})
			}
		})
	}
}

func TestHashVehicle(t *testing.T) {
	for _, tc := range []struct {
		field  string
		getter func(v *Vehicle) any
	}{
		{
			"id",
			func(v *Vehicle) any {
				return &v.ID
			},
		},
		{
			"id.id",
			func(v *Vehicle) any {
				return &v.ID.ID
			},
		},
		{
			"id.label",
			func(v *Vehicle) any {
				return &v.ID.Label
			},
		},
		{
			"id.licence_plate",
			func(v *Vehicle) any {
				return &v.ID.LicencePlate
			},
		},
		{
			"trip",
			func(v *Vehicle) any {
				return &v.Trip
			},
		},
		{
			"position",
			func(v *Vehicle) any {
				return &v.Position
			},
		},
		{
			"position.longitude",
			func(v *Vehicle) any {
				return &v.Position.Longitude
			},
		},
		{
			"position.bearing",
			func(v *Vehicle) any {
				return &v.Position.Bearing
			},
		},
		{
			"position.odometer",
			func(v *Vehicle) any {
				return &v.Position.Odometer
			},
		},
		{
			"position.speed",
			func(v *Vehicle) any {
				return &v.Position.Speed
			},
		},
		{
			"current_stop_sequence",
			func(v *Vehicle) any {
				return &v.CurrentStopSequence
			},
		},
		{
			"stop_id",
			func(v *Vehicle) any {
				return &v.StopID
			},
		},
		{
			"current_status",
			func(v *Vehicle) any {
				return &v.CurrentStatus
			},
		},
		{
			"timestamp",
			func(v *Vehicle) any {
				return &v.Timestamp
			},
		},
		{
			"congestion_level",
			func(v *Vehicle) any {
				return &v.CongestionLevel
			},
		},
		{
			"occupancy_status",
			func(v *Vehicle) any {
				return &v.OccupancyStatus
			},
		},
		{
			"occupancy_percentage",
			func(v *Vehicle) any {
				return &v.OccupancyPercentage
			},
		},
	} {
		t.Run(tc.field, func(t *testing.T) {
			vehicle := mkVehicle()
			modifierPairs := combinations(allModifiers(tc.getter(&vehicle)))
			for _, pair := range modifierPairs {
				t.Run(fmt.Sprintf("%s-%s", pair[0].name, pair[1].name), func(t *testing.T) {
					vehicle1 := mkVehicle()
					pair[0].fn(tc.getter(&vehicle1))
					h1 := md5.New()
					vehicle1.Hash(h1)
					s1 := fmt.Sprintf("%x", h1.Sum(nil))

					vehicle2 := mkVehicle()
					pair[1].fn(tc.getter(&vehicle2))
					h2 := md5.New()
					vehicle2.Hash(h2)
					s2 := fmt.Sprintf("%x", h2.Sum(nil))

					if s1 == s2 {
						t.Errorf("hashes match but vehicles are different\nvehicle1: %v\nvehicle2: %v", vehicle1, vehicle2)
					}
				})
			}
		})
	}
}

func mkTrip(i int) Trip {
	return Trip{
		ID: TripID{
			ID:           "id.id",
			RouteID:      "route.id",
			HasStartDate: true,
			StartDate:    mkTime(i + 1),
			HasStartTime: true,
			StartTime:    mkDuration(i + 2),
		},
		StopTimeUpdates: []StopTimeUpdate{
			{
				StopSequence: ptr(uint32(i + 3)),
				StopID:       ptr("stop_time_updates.0.stop_id"),
				Arrival: &StopTimeEvent{
					Time:        ptr(mkTime(i + 4)),
					Delay:       ptr(mkDuration(i + 5)),
					Uncertainty: ptr(int32(i + 6)),
				},
				Departure: &StopTimeEvent{
					Time:        ptr(mkTime(i + 7)),
					Delay:       ptr(time.Hour * mkDuration(i+8)),
					Uncertainty: ptr(int32(i + 9)),
				},
				NyctTrack: ptr("stop_time_updates.0.nyct_track"),
			},
		},
		NyctIsAssigned: true,
	}
}

func mkVehicle() Vehicle {
	return Vehicle{
		ID: &VehicleID{
			ID:           ptr("vehicle.id.id"),
			Label:        ptr("vehicle.id.label"),
			LicencePlate: ptr("vehicle.id.licence_plate"),
		},
		Trip:                ptr(mkTrip(0)),
		Position:            ptr(mkPosition(9)),
		CurrentStopSequence: ptr(uint32(15)),
		StopID:              ptr("vehicle.stop_id"),
		CurrentStatus:       ptr(gtfsrt.VehiclePosition_IN_TRANSIT_TO),
		Timestamp:           ptr(mkTime(16)),
		CongestionLevel:     gtfsrt.VehiclePosition_UNKNOWN_CONGESTION_LEVEL,
		OccupancyStatus:     ptr(gtfsrt.VehiclePosition_FULL),
		OccupancyPercentage: ptr(uint32(17)),
	}
}

func mkPosition(i float32) Position {
	return Position{
		Latitude:  ptr(float32(i + 1.0)),
		Longitude: ptr(float32(i + 2.0)),
		Bearing:   ptr(float32(i + 3.0)),
		Odometer:  ptr(float64(i + 4.0)),
		Speed:     ptr(float32(i + 5.0)),
	}
}

func mkTime(i int) time.Time {
	return time.Date(2023, time.April, 24, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Hour)
}

func mkDuration(i int) time.Duration {
	return time.Hour * time.Duration(i)
}

type modifier struct {
	name string
	fn   func(a any)
}

func allModifiers(a any) []modifier {
	noOpModifier := modifier{name: "no op", fn: noOpModifierFn}
	otherValueModifier := modifier{name: "other value", fn: otherValueModifierFn}
	zeroModifier := modifier{name: "zero value", fn: zeroModifierFn}
	nilModifier := modifier{name: "nil value", fn: nilModifierFn}
	switch a.(type) {
	case *bool:
		return []modifier{noOpModifier, otherValueModifier}
	case **int32:
		return []modifier{noOpModifier, otherValueModifier, zeroModifier, nilModifier}
	case **uint32:
		return []modifier{noOpModifier, otherValueModifier, zeroModifier, nilModifier}
	case **float32:
		return []modifier{noOpModifier, otherValueModifier, zeroModifier, nilModifier}
	case **float64:
		return []modifier{noOpModifier, otherValueModifier, zeroModifier, nilModifier}
	case *string:
		return []modifier{noOpModifier, otherValueModifier, zeroModifier}
	case **string:
		return []modifier{noOpModifier, otherValueModifier, zeroModifier, nilModifier}
	case *time.Duration:
		return []modifier{noOpModifier, otherValueModifier, zeroModifier}
	case **time.Duration:
		return []modifier{noOpModifier, otherValueModifier, zeroModifier, nilModifier}
	case *time.Time:
		return []modifier{noOpModifier, otherValueModifier, zeroModifier}
	case **time.Time:
		return []modifier{noOpModifier, otherValueModifier, zeroModifier, nilModifier}
	case **StopTimeEvent:
		return []modifier{noOpModifier, nilModifier}
	case **VehicleID:
		return []modifier{noOpModifier, nilModifier}
	case **Trip:
		return []modifier{noOpModifier, otherValueModifier, nilModifier}
	case **Position:
		return []modifier{noOpModifier, otherValueModifier, nilModifier}
	case **CurrentStatus:
		return []modifier{noOpModifier, otherValueModifier, nilModifier}
	case *CongestionLevel:
		return []modifier{noOpModifier, otherValueModifier}
	case **OccupancyStatus:
		return []modifier{noOpModifier, otherValueModifier, nilModifier}
	default:
		panic(fmt.Sprintf("invalid type %T", a))
	}
}

func noOpModifierFn(a any) {}

func zeroModifierFn(a any) {
	switch t := a.(type) {
	case **int32:
		*t = ptr(int32(0))
	case **uint32:
		*t = ptr(uint32(0))
	case **float32:
		*t = ptr(float32(0))
	case **float64:
		*t = ptr(float64(0))
	case *string:
		*t = ""
	case **string:
		*t = ptr("")
	case *time.Duration:
		var d time.Duration
		*t = d
	case **time.Duration:
		var d time.Duration
		*t = ptr(d)
	case *time.Time:
		var ti time.Time
		*t = ti
	case **time.Time:
		var ti time.Time
		*t = ptr(ti)
	default:
		panic(fmt.Sprintf("invalid type %T", a))
	}
}

func nilModifierFn(a any) {
	switch t := a.(type) {
	case **int32:
		*t = nil
	case **uint32:
		*t = nil
	case **float32:
		*t = nil
	case **float64:
		*t = nil
	case **string:
		*t = nil
	case **time.Time:
		*t = nil
	case **time.Duration:
		*t = nil
	case **StopTimeEvent:
		*t = nil
	case **VehicleID:
		*t = nil
	case **Trip:
		*t = nil
	case **Position:
		*t = nil
	case **CurrentStatus:
		*t = nil
	case **OccupancyStatus:
		*t = nil
	default:
		panic(fmt.Sprintf("invalid type %T", a))
	}
}

func otherValueModifierFn(a any) {
	switch t := a.(type) {
	case *bool:
		*t = false
	case **int32:
		*t = ptr(int32(101))
	case **uint32:
		*t = ptr(uint32(102))
	case **float32:
		*t = ptr(float32(103))
	case **float64:
		*t = ptr(float64(104))
	case *string:
		*t = "other"
	case **string:
		*t = ptr("other")
	case *time.Duration:
		*t = mkDuration(105)
	case **time.Duration:
		*t = ptr(mkDuration(106))
	case *time.Time:
		*t = mkTime(107)
	case **time.Time:
		*t = ptr(mkTime(108))
	case **Trip:
		*t = ptr(mkTrip(109))
	case **Position:
		*t = ptr(mkPosition(110))
	case **CurrentStatus:
		*t = ptr(gtfsrt.VehiclePosition_STOPPED_AT)
	case *CongestionLevel:
		*t = gtfsrt.VehiclePosition_STOP_AND_GO
	case **OccupancyStatus:
		*t = ptr(gtfsrt.VehiclePosition_CRUSHED_STANDING_ROOM_ONLY)
	default:
		panic(fmt.Sprintf("invalid type %T", a))
	}
}

func combinations(m []modifier) [][2]modifier {
	var c [][2]modifier
	for i, lhs := range m {
		for _, rhs := range m[i+1:] {
			c = append(c, [2]modifier{lhs, rhs})
		}
	}
	return c
}
