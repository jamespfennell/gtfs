package gtfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
)

// Hash calculates a hash of a trip using the provided hash function.
//
// The Vehicle and IsEntityInFeed fields are ignored for the purposes of hashing.
func (t *Trip) Hash(h hash.Hash) {
	s := hasher{h: h}
	s.trip(t)
	s.flush()
}

// Hash calculates a hash of a vehicle using the provided hash function.
//
// The Trip and IsEntityInFeed fields are ignored for the purposes of hashing.
func (v *Vehicle) Hash(h hash.Hash) {
	s := hasher{h: h}
	s.vehicle(v)
	s.flush()
}

type hasher struct {
	h hash.Hash
	b bytes.Buffer
}

func (h *hasher) flush() {
	h.h.Write(h.b.Bytes())
	h.b.Reset()
}

func (h *hasher) trip(t *Trip) {
	h.string(t.ID.ID)
	h.string(t.ID.RouteID)
	h.number(t.ID.HasStartDate)
	h.number(t.ID.StartDate.Unix())
	h.number(t.ID.HasStartTime)
	h.number(t.ID.StartTime)
	h.number(t.NyctIsAssigned)
	h.number(int64(len(t.StopTimeUpdates)))
	for i := range t.StopTimeUpdates {
		stu := &t.StopTimeUpdates[i]
		hashNumberPtr(h, stu.StopSequence)
		h.stringPtr(stu.StopID)
		h.stringPtr(stu.NyctTrack)
		for _, event := range []*StopTimeEvent{stu.Arrival, stu.Departure} {
			h.number(event == nil)
			if event == nil {
				continue
			}
			var up *int64
			if event.Time != nil {
				u := event.Time.Unix()
				up = &u
			}
			hashNumberPtr(h, up)
			var dp *int64
			if event.Delay != nil {
				d := int64(*event.Delay)
				dp = &d
			}
			hashNumberPtr(h, dp)
			hashNumberPtr(h, event.Uncertainty)
		}
	}
}

func (h *hasher) vehicle(v *Vehicle) {
	h.number(v.ID == nil)
	if v.ID != nil {
		h.string(v.ID.ID)
		h.string(v.ID.Label)
		h.string(v.ID.LicencePlate)
	}
}

func (h *hasher) string(s string) {
	h.number(uint64(len(s)))
	h.flush()
	h.h.Write([]byte(s))
}

func hashNumberPtr[T any](h *hasher, a *T) {
	h.number(a == nil)
	if a != nil {
		h.number(*a)
	}
}

func (h *hasher) stringPtr(a *string) {
	h.number(a == nil)
	if a != nil {
		h.string(*a)
	}
}

func (h *hasher) number(a any) {
	err := binary.Write(&h.b, binary.LittleEndian, a)
	if err != nil {
		panic(fmt.Sprintf("failed to hash %T", a))
	}
}
