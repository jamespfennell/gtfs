package gtfs_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/jamespfennell/gtfs"
	"github.com/jamespfennell/gtfs/internal/testutil"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
)

const tripID1 = "tripID1"
const vehicleID1 = "vehicleID1"

var time1 time.Time = time.Unix(2<<28, 0).UTC()

func TestSoloTrip(t *testing.T) {
	timestamp := uint64(time1.Unix())
	header := &gtfsrt.FeedHeader{
		GtfsRealtimeVersion: ptr("2.0"),
		Timestamp:           &timestamp,
	}
	entities := []*gtfsrt.FeedEntity{
		{
			Id: ptr("1"),
			TripUpdate: &gtfsrt.TripUpdate{
				Trip: &gtfsrt.TripDescriptor{
					TripId: ptr(tripID1),
				},
				Vehicle: &gtfsrt.VehicleDescriptor{
					Id: ptr(vehicleID1),
				},
			},
		},
	}

	trip := gtfs.Trip{
		ID: gtfs.TripID{
			ID:          tripID1,
			DirectionID: gtfs.DirectionIDUnspecified,
		},
		IsEntityInMessage: true,
	}
	vehicle := gtfs.Vehicle{
		ID: &gtfs.VehicleID{
			ID: vehicleID1,
		},
		IsEntityInMessage: false,
	}
	trip.Vehicle = &vehicle
	vehicle.Trip = &trip
	expectedResult := &gtfs.Realtime{
		CreatedAt: time1,
		Trips:     []gtfs.Trip{trip},
		Vehicles:  []gtfs.Vehicle{vehicle},
	}

	result := testutil.MustParse(t, header, entities, &gtfs.ParseRealtimeOptions{})

	if !reflect.DeepEqual(result, expectedResult) {
		t.Errorf("actual:\n%+v\n!= expected:\n%+v", result, expectedResult)
	}
}

func TestAlert(t *testing.T) {
	start := uint64(100)
	startT := time.Unix(100, 0).UTC()
	end := uint64(200)
	endT := time.Unix(200, 0).UTC()
	cause := gtfsrt.Alert_CONSTRUCTION
	effect := gtfsrt.Alert_SIGNIFICANT_DELAYS
	entities := []*gtfsrt.FeedEntity{
		{
			Id: ptr("AlertID"),
			Alert: &gtfsrt.Alert{
				ActivePeriod: []*gtfsrt.TimeRange{
					{
						Start: &start,
						End:   &end,
					},
				},
				InformedEntity: []*gtfsrt.EntitySelector{
					{
						AgencyId: ptr("AgencyID"),
					},
					{
						RouteId: ptr("RouteID"),
					},
					{
						StopId: ptr("StopID"),
					},
				},
				Cause:  &cause,
				Effect: &effect,
				Url: &gtfsrt.TranslatedString{
					Translation: []*gtfsrt.TranslatedString_Translation{
						{
							Text:     ptr("UrlText"),
							Language: ptr("UrlLanguage"),
						},
					},
				},
				HeaderText: &gtfsrt.TranslatedString{
					Translation: []*gtfsrt.TranslatedString_Translation{
						{
							Text:     ptr("HeaderText"),
							Language: ptr("HeaderLanguage"),
						},
					},
				},
				DescriptionText: &gtfsrt.TranslatedString{
					Translation: []*gtfsrt.TranslatedString_Translation{
						{
							Text:     ptr("DescriptionText"),
							Language: ptr("DescriptionLanguage"),
						},
					},
				},
				// TODO: other fields
			},
		},
	}

	wantAlert := gtfs.Alert{
		ID: "AlertID",
		ActivePeriods: []gtfs.AlertActivePeriod{
			{
				StartsAt: &startT,
				EndsAt:   &endT,
			},
		},
		InformedEntities: []gtfs.AlertInformedEntity{
			{
				AgencyID: ptr("AgencyID"),
			},
			{
				RouteID: ptr("RouteID"),
			},
			{
				StopID: ptr("StopID"),
			},
		},
		Cause:  cause,
		Effect: effect,
		URL: []gtfs.AlertText{
			{
				Text:     "UrlText",
				Language: "UrlLanguage",
			},
		},
		Header: []gtfs.AlertText{
			{
				Text:     "HeaderText",
				Language: "HeaderLanguage",
			},
		},
		Description: []gtfs.AlertText{
			{
				Text:     "DescriptionText",
				Language: "DescriptionLanguage",
			},
		},
	}

	result := testutil.MustParse(t, nil, entities, &gtfs.ParseRealtimeOptions{})

	if !reflect.DeepEqual(result.Alerts, []gtfs.Alert{wantAlert}) {
		t.Errorf("actual:\n%+v\n!= expected:\n%+v", result.Alerts, []gtfs.Alert{wantAlert})
	}
}

func ptr(s string) *string {
	return &s
}
