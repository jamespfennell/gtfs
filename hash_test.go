package gtfs

import (
	"crypto/md5"
	"fmt"
	"testing"
	"time"
)

func BenchmarkHashTrip(b *testing.B) {
	trip := mkTrip()
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
			trip := mkTrip()
			modifierPairs := combinations(allModifiers(tc.getter(&trip)))
			for _, pair := range modifierPairs {
				t.Run(fmt.Sprintf("%s-%s", pair[0].name, pair[1].name), func(t *testing.T) {
					trip1 := mkTrip()
					pair[0].fn(tc.getter(&trip1))
					h1 := md5.New()
					trip1.Hash(h1)
					s1 := fmt.Sprintf("%x", h1.Sum(nil))

					trip2 := mkTrip()
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
						t.Errorf("hashes match but trips are different\nvehicle1: %v\nvehicle2: %v", vehicle1, vehicle2)
					}
				})
			}
		})
	}
}

func mkTrip() Trip {
	return Trip{
		ID: TripID{
			ID:           "id.id",
			RouteID:      "route.id",
			HasStartDate: true,
			StartDate:    mkTime(1),
			HasStartTime: true,
			StartTime:    mkDuration(2),
		},
		StopTimeUpdates: []StopTimeUpdate{
			{
				StopSequence: ptr(uint32(3)),
				StopID:       ptr("stop_time_updates.0.stop_id"),
				Arrival: &StopTimeEvent{
					Time:        ptr(mkTime(4)),
					Delay:       ptr(mkDuration(5)),
					Uncertainty: ptr(int32(6)),
				},
				Departure: &StopTimeEvent{
					Time:        ptr(mkTime(7)),
					Delay:       ptr(time.Hour * 8),
					Uncertainty: ptr(int32(9)),
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
			ID:           "vehicle.id.id",
			Label:        "vehicle.id.label",
			LicencePlate: "vehicle.id.licence_plate",
		},
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
	case *string:
		*t = "other"
	case **string:
		*t = ptr("other")
	case *time.Duration:
		*t = mkDuration(103)
	case **time.Duration:
		*t = ptr(mkDuration(104))
	case *time.Time:
		*t = mkTime(105)
	case **time.Time:
		*t = ptr(mkTime(106))
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
