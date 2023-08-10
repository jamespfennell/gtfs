package gtfs

import (
	"archive/zip"
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

var (
	may4 = time.Date(2022, 5, 4, 0, 0, 0, 0, time.UTC)
	may7 = time.Date(2022, 5, 7, 0, 0, 0, 0, time.UTC)
)

func TestParse(t *testing.T) {
	defaultAgency := Agency{
		Id:       "a",
		Name:     "b",
		Url:      "c",
		Timezone: "d",
	}
	otherAgency := Agency{
		Id:       "e",
		Name:     "f",
		Url:      "g",
		Timezone: "h",
	}
	defaultRoute := Route{
		Id:                "route_id",
		Agency:            &defaultAgency,
		Color:             "FFFFFF",
		TextColor:         "000000",
		Type:              RouteType_Bus,
		ContinuousPickup:  PickupDropOffPolicy_No,
		ContinuousDropOff: PickupDropOffPolicy_No,
	}
	defaultStop := Stop{
		Id: "stop_id",
	}
	defaultService := Service{
		Id:        "service_id",
		StartDate: may4,
		EndDate:   may7,
	}
	defaultTrip := ScheduledTrip{
		ID:      "trip_id",
		Route:   &defaultRoute,
		Service: &defaultService,
	}
	for _, tc := range []struct {
		desc     string
		content  []byte
		opts     ParseStaticOptions
		expected *Static
	}{
		{
			desc: "agency with only required fields",
			content: newZipBuilder().add(
				"agency.txt",
				"agency_id,agency_name,agency_url,agency_timezone\na,b,c,d",
			).build(),
			expected: &Static{
				Agencies: []Agency{
					{
						Id:       "a",
						Name:     "b",
						Url:      "c",
						Timezone: "d",
					},
				},
			},
		},
		{
			desc: "agency with all fields",
			content: newZipBuilder().add(
				"agency.txt",
				"agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url,agency_email\na,b,c,d,e,f,g,h",
			).build(),
			expected: &Static{
				Agencies: []Agency{
					{
						Id:       "a",
						Name:     "b",
						Url:      "c",
						Timezone: "d",
						Language: "e",
						Phone:    "f",
						FareUrl:  "g",
						Email:    "h",
					},
				},
			},
		},
		{
			desc: "route with only required fields",
			content: newZipBuilder().add(
				"agency.txt",
				"agency_id,agency_name,agency_url,agency_timezone\na,b,c,d",
			).add(
				"routes.txt",
				"route_id,route_type\na,3",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes: []Route{
					{
						Id:                "a",
						Agency:            &defaultAgency,
						Color:             "FFFFFF",
						TextColor:         "000000",
						Type:              RouteType_Bus,
						ContinuousPickup:  PickupDropOffPolicy_No,
						ContinuousDropOff: PickupDropOffPolicy_No,
					},
				},
			},
		},
		{
			desc: "route with all fields",
			content: newZipBuilder().add(
				"agency.txt",
				"agency_id,agency_name,agency_url,agency_timezone\na,b,c,d",
			).add(
				"routes.txt",
				"route_id,route_color,route_text_color,route_short_name,"+
					"route_long_name,route_desc,route_type,route_url,route_sort_order,continuous_pickup,continuous_drop_off\n"+
					"a,b,c,e,f,g,2,h,5,0,2",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes: []Route{
					{
						Id:                "a",
						Agency:            &defaultAgency,
						Color:             "b",
						TextColor:         "c",
						ShortName:         "e",
						LongName:          "f",
						Description:       "g",
						Type:              RouteType_Rail,
						Url:               "h",
						SortOrder:         ptr(int32(5)),
						ContinuousPickup:  PickupDropOffPolicy_Yes,
						ContinuousDropOff: PickupDropOffPolicy_PhoneAgency,
					},
				},
			},
		},
		{
			desc: "route with matching specified agency",
			content: newZipBuilder().add(
				"agency.txt",
				"agency_id,agency_name,agency_url,agency_timezone\na,b,c,d\ne,f,g,h",
			).add(
				"routes.txt",
				"route_id,route_type,agency_id\na,3,e",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency, otherAgency},
				Routes: []Route{
					{
						Id:                "a",
						Agency:            &otherAgency,
						Color:             "FFFFFF",
						TextColor:         "000000",
						Type:              RouteType_Bus,
						ContinuousPickup:  PickupDropOffPolicy_No,
						ContinuousDropOff: PickupDropOffPolicy_No,
					},
				},
			},
		},
		{
			desc: "stop",
			content: newZipBuilder().add(
				"stops.txt",
				"stop_id,stop_code,stop_name,stop_desc,zone_id,stop_lon,stop_lat,"+
					"stop_url,location_type,stop_timezone,wheelchair_boarding,platform_code",
				"a,b,c,d,e,1.5,2.5,f,1,g,1,h",
				"i,j,k,l,m,1.5,2.5,n,1,o,1,p",
			).build(),
			expected: &Static{
				Stops: []Stop{
					{
						Id:                 "a",
						Code:               "b",
						Name:               "c",
						Description:        "d",
						ZoneId:             "e",
						Longitude:          ptr(1.5),
						Latitude:           ptr(2.5),
						Url:                "f",
						Type:               StopType_Station,
						Timezone:           "g",
						WheelchairBoarding: WheelchairBoarding_Possible,
						PlatformCode:       "h",
					},
					{
						Id:                 "i",
						Code:               "j",
						Name:               "k",
						Description:        "l",
						ZoneId:             "m",
						Longitude:          ptr(1.5),
						Latitude:           ptr(2.5),
						Url:                "n",
						Type:               StopType_Station,
						Timezone:           "o",
						WheelchairBoarding: WheelchairBoarding_Possible,
						PlatformCode:       "p",
					},
				},
			},
		},
		{
			desc: "stop with parent",
			content: newZipBuilder().add(
				"stops.txt",
				"stop_id,parent_station\na,b\nb,",
			).build(),
			expected: &Static{
				Stops: []Stop{
					{
						Id:     "a",
						Parent: &Stop{Id: "b"},
					},
					{
						Id: "b",
					},
				},
			},
		},
		{
			desc: "transfer",
			content: newZipBuilder().add(
				"stops.txt",
				"stop_id\na\nb",
			).add(
				"transfers.txt",
				"from_stop_id,to_stop_id,transfer_type,min_transfer_time\na,b,2,300",
			).build(),
			expected: &Static{
				Stops: []Stop{
					{Id: "a"},
					{Id: "b"},
				},
				Transfers: []Transfer{
					{
						From:            &Stop{Id: "a"},
						To:              &Stop{Id: "b"},
						Type:            RequiresTime,
						MinTransferTime: ptr(int32(300)),
					},
				},
			},
		},
		{
			desc: "same stop transfer",
			content: newZipBuilder().add(
				"stops.txt",
				"stop_id\na",
			).add(
				"transfers.txt",
				"from_stop_id,to_stop_id\na,a",
			).build(),
			expected: &Static{
				Stops: []Stop{{Id: "a"}},
			},
		},
		{
			desc: "transfer unknown to_id",
			content: newZipBuilder().add(
				"stops.txt",
				"stop_id\na",
			).add(
				"transfers.txt",
				"from_stop_id,to_stop_id\na,b",
			).build(),
			expected: &Static{
				Stops: []Stop{{Id: "a"}},
			},
		},
		{
			desc: "transfer unknown from_id",
			content: newZipBuilder().add(
				"stops.txt",
				"stop_id\nb",
			).add(
				"transfers.txt",
				"from_stop_id,to_stop_id\na,b",
			).build(),
			expected: &Static{
				Stops: []Stop{{Id: "b"}},
			},
		},
		{
			desc: "calendar.txt",
			content: newZipBuilder().add(
				"calendar.txt",
				"service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"+
					"a,1,0,1,0,1,0,1,20220504,20220507",
			).build(),
			expected: &Static{
				Services: []Service{
					{
						Id:        "a",
						Monday:    true,
						Tuesday:   false,
						Wednesday: true,
						Thursday:  false,
						Friday:    true,
						Saturday:  false,
						Sunday:    true,
						StartDate: may4,
						EndDate:   may7,
					},
				},
			},
		},
		{
			desc: "calendar_dates.txt",
			content: newZipBuilder().add(
				"calendar_dates.txt",
				"service_id,date,exception_type\na,20220504,1\na,20220507,2",
			).build(),
			expected: &Static{
				Services: []Service{
					{
						Id:           "a",
						StartDate:    may4,
						EndDate:      may7,
						AddedDates:   []time.Time{may4},
						RemovedDates: []time.Time{may7},
					},
				},
			},
		},
		{
			desc: "trip",
			content: newZipBuilder().add(
				"agency.txt",
				"agency_id,agency_name,agency_url,agency_timezone\na,b,c,d",
			).add(
				"routes.txt",
				"route_id,route_type\nroute_id,3",
			).add(
				"stops.txt",
				"stop_id\nstop_id",
			).add(
				"calendar.txt",
				"service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"+
					"service_id,0,0,0,0,0,0,0,20220504,20220507",
			).add(
				"trips.txt",
				"route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,wheelchair_accessible,bikes_allowed\n"+
					"route_id,service_id,a,b,c,1,block_id,0,2",
			).add(
				"stop_times.txt",
				"stop_id,trip_id,arrival_time,departure_time,stop_sequence,stop_headsign,pickup_type,drop_off_type,continuous_pickup,continuous_drop_off,shape_dist_traveled,timepoint\n"+
					"stop_id,a,04:05:06,13:14:15,50,b,0,1,2,3,0.25,1",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes:   []Route{defaultRoute},
				Services: []Service{defaultService},
				Stops:    []Stop{defaultStop},
				Trips: []ScheduledTrip{
					{
						Route:                &defaultRoute,
						Service:              &defaultService,
						ID:                   "a",
						Headsign:             "b",
						ShortName:            "c",
						DirectionId:          DirectionIDTrue,
						BlockID:              "block_id",
						WheelchairAccessible: WheelchairBoarding_NotSpecified,
						BikesAllowed:         BikesAllowed_NotAllowed,
						StopTimes: []ScheduledStopTime{
							{
								Stop:                  &defaultStop,
								Headsign:              "b",
								StopSequence:          50,
								ArrivalTime:           4*time.Hour + 5*time.Minute + 6*time.Second,
								DepartureTime:         13*time.Hour + 14*time.Minute + 15*time.Second,
								PickupType:            PickupDropOffPolicy_Yes,
								DropOffType:           PickupDropOffPolicy_No,
								ContinuousPickup:      PickupDropOffPolicy_PhoneAgency,
								ContinuousDropOff:     PickupDropOffPolicy_CoordinateWithDriver,
								ShapeDistanceTraveled: ptr(0.25),
								ExactTimes:            true,
							},
						},
					},
				},
			},
		},
		{
			desc: "stop with spaces in lat/lon",
			content: newZipBuilder().add(
				"stops.txt",
				"stop_id,stop_code,stop_name,stop_desc,zone_id,stop_lon,stop_lat,"+
					"stop_url,location_type,stop_timezone,wheelchair_boarding,platform_code\n"+
					"a,b,c,d,e, 1.5 , 2.5 ,f,1,g,1,h",
			).build(),
			expected: &Static{
				Stops: []Stop{
					{
						Id:                 "a",
						Code:               "b",
						Name:               "c",
						Description:        "d",
						ZoneId:             "e",
						Longitude:          ptr(1.5),
						Latitude:           ptr(2.5),
						Url:                "f",
						Type:               StopType_Station,
						Timezone:           "g",
						WheelchairBoarding: WheelchairBoarding_Possible,
						PlatformCode:       "h",
					},
				},
			},
		},
		{
			desc: "data with BOM",
			content: newZipBuilder().add(
				"stops.txt",
				"\xEF\xBB\xBFstop_id,stop_code,stop_name,stop_desc,zone_id,stop_lon,stop_lat,"+
					"stop_url,location_type,stop_timezone,wheelchair_boarding,platform_code\n"+
					"a,b,c,d,e,1.5,2.5,f,1,g,1,h",
			).build(),
			expected: &Static{
				Stops: []Stop{
					{
						Id:                 "a",
						Code:               "b",
						Name:               "c",
						Description:        "d",
						ZoneId:             "e",
						Longitude:          ptr(1.5),
						Latitude:           ptr(2.5),
						Url:                "f",
						Type:               StopType_Station,
						Timezone:           "g",
						WheelchairBoarding: WheelchairBoarding_Possible,
						PlatformCode:       "h",
					},
				},
			},
		},
		{
			desc: "empty shapes file",
			content: newZipBuilderWithDefaults().add(
				"shapes.txt",
				"shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence",
			).add(
				"trips.txt",
				"route_id,service_id,trip_id,shape_id",
				"route_id,service_id,trip_id,shape_id",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes:   []Route{defaultRoute},
				Services: []Service{defaultService},
				Stops:    []Stop{defaultStop},
				Trips: []ScheduledTrip{
					{
						Route:   &defaultRoute,
						Service: &defaultService,
						ID:      "trip_id",
					},
				},
				Shapes: []Shape{},
			},
		},
		{
			desc: "single point shape",
			content: newZipBuilderWithDefaults().add(
				"shapes.txt",
				"shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence",
				"shape_1,1.5,2.5,1",
			).add(
				"trips.txt",
				"route_id,service_id,trip_id,shape_id",
				"route_id,service_id,trip_id,shape_1",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes:   []Route{defaultRoute},
				Services: []Service{defaultService},
				Stops:    []Stop{defaultStop},
				Trips: []ScheduledTrip{
					{
						Route:   &defaultRoute,
						Service: &defaultService,
						ID:      "trip_id",
						Shape: &Shape{
							ID: "shape_1",
							Points: []ShapePoint{
								{
									Latitude:  1.5,
									Longitude: 2.5,
								},
							},
						},
					},
				},
				Shapes: []Shape{
					{
						ID: "shape_1",
						Points: []ShapePoint{
							{
								Latitude:  1.5,
								Longitude: 2.5,
							},
						},
					},
				},
			},
		},
		{
			desc: "multi point shape",
			content: newZipBuilderWithDefaults().add(
				"shapes.txt",
				"shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence",
				"shape_1,1.5,2.5,1",
				"shape_1,2.5,3.5,2",
				"shape_1,3.5,4.5,3",
			).add(
				"trips.txt",
				"route_id,service_id,trip_id,shape_id",
				"route_id,service_id,trip_id,shape_1",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes:   []Route{defaultRoute},
				Services: []Service{defaultService},
				Stops:    []Stop{defaultStop},
				Trips: []ScheduledTrip{
					{
						Route:   &defaultRoute,
						Service: &defaultService,
						ID:      "trip_id",
						Shape: &Shape{
							ID: "shape_1",
							Points: []ShapePoint{
								{
									Latitude:  1.5,
									Longitude: 2.5,
								},
								{
									Latitude:  2.5,
									Longitude: 3.5,
								},
								{
									Latitude:  3.5,
									Longitude: 4.5,
								},
							},
						},
					},
				},
				Shapes: []Shape{
					{
						ID: "shape_1",
						Points: []ShapePoint{
							{
								Latitude:  1.5,
								Longitude: 2.5,
							},
							{
								Latitude:  2.5,
								Longitude: 3.5,
							},
							{
								Latitude:  3.5,
								Longitude: 4.5,
							},
						},
					},
				},
			},
		},
		{
			desc: "points not in row order",
			content: newZipBuilderWithDefaults().add(
				"shapes.txt",
				"shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence",
				"shape_1,3.5,4.5,3",
				"shape_1,2.5,3.5,2",
				"shape_1,1.5,2.5,1",
			).add(
				"trips.txt",
				"route_id,service_id,trip_id,shape_id",
				"route_id,service_id,trip_id,shape_1",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes:   []Route{defaultRoute},
				Services: []Service{defaultService},
				Stops:    []Stop{defaultStop},
				Trips: []ScheduledTrip{
					{
						Route:   &defaultRoute,
						Service: &defaultService,
						ID:      "trip_id",
						Shape: &Shape{
							ID: "shape_1",
							Points: []ShapePoint{
								{
									Latitude:  1.5,
									Longitude: 2.5,
								},
								{
									Latitude:  2.5,
									Longitude: 3.5,
								},
								{
									Latitude:  3.5,
									Longitude: 4.5,
								},
							},
						},
					},
				},
				Shapes: []Shape{
					{
						ID: "shape_1",
						Points: []ShapePoint{
							{
								Latitude:  1.5,
								Longitude: 2.5,
							},
							{
								Latitude:  2.5,
								Longitude: 3.5,
							},
							{
								Latitude:  3.5,
								Longitude: 4.5,
							},
						},
					},
				},
			},
		},
		{
			desc: "multiple shapes",
			content: newZipBuilderWithDefaults().add(
				"shapes.txt",
				"shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence",
				"shape_1,1.5,2.5,1",
				"shape_1,2.5,3.5,2",
				"shape_1,3.5,4.5,3",
				"shape_2,4.5,5.5,1",
				"shape_2,5.5,6.5,2",
			).add(
				"trips.txt",
				"route_id,service_id,trip_id,shape_id",
				"route_id,service_id,trip_1,shape_1",
				"route_id,service_id,trip_2,shape_2",
				"route_id,service_id,trip_3,shape_1",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes:   []Route{defaultRoute},
				Services: []Service{defaultService},
				Stops:    []Stop{defaultStop},
				Trips: []ScheduledTrip{
					{
						Route:   &defaultRoute,
						Service: &defaultService,
						ID:      "trip_1",
						Shape: &Shape{
							ID: "shape_1",
							Points: []ShapePoint{
								{
									Latitude:  1.5,
									Longitude: 2.5,
								},
								{
									Latitude:  2.5,
									Longitude: 3.5,
								},
								{
									Latitude:  3.5,
									Longitude: 4.5,
								},
							},
						},
					},
					{
						Route:   &defaultRoute,
						Service: &defaultService,
						ID:      "trip_2",
						Shape: &Shape{
							ID: "shape_2",
							Points: []ShapePoint{
								{
									Latitude:  4.5,
									Longitude: 5.5,
								},
								{
									Latitude:  5.5,
									Longitude: 6.5,
								},
							},
						},
					},
					{
						Route:   &defaultRoute,
						Service: &defaultService,
						ID:      "trip_3",
						Shape: &Shape{
							ID: "shape_1",
							Points: []ShapePoint{
								{
									Latitude:  1.5,
									Longitude: 2.5,
								},
								{
									Latitude:  2.5,
									Longitude: 3.5,
								},
								{
									Latitude:  3.5,
									Longitude: 4.5,
								},
							},
						},
					},
				},
				Shapes: []Shape{
					{
						ID: "shape_1",
						Points: []ShapePoint{
							{
								Latitude:  1.5,
								Longitude: 2.5,
							},
							{
								Latitude:  2.5,
								Longitude: 3.5,
							},
							{
								Latitude:  3.5,
								Longitude: 4.5,
							},
						},
					},
					{
						ID: "shape_2",
						Points: []ShapePoint{
							{
								Latitude:  4.5,
								Longitude: 5.5,
							},
							{
								Latitude:  5.5,
								Longitude: 6.5,
							},
						},
					},
				},
			},
		},
		{
			desc: "shape dist traveled",
			content: newZipBuilderWithDefaults().add(
				"shapes.txt",
				"shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled",
				"shape_1,1.5,2.5,1,0",
				"shape_1,2.5,3.5,2,",
				"shape_1,3.5,4.5,3,20",
			).add(
				"trips.txt",
				"route_id,service_id,trip_id,shape_id",
				"route_id,service_id,trip_1,shape_1",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes:   []Route{defaultRoute},
				Services: []Service{defaultService},
				Stops:    []Stop{defaultStop},
				Trips: []ScheduledTrip{
					{
						Route:   &defaultRoute,
						Service: &defaultService,
						ID:      "trip_1",
						Shape: &Shape{
							ID: "shape_1",
							Points: []ShapePoint{
								{
									Latitude:  1.5,
									Longitude: 2.5,
									Distance:  ptr(float64(0)),
								},
								{
									Latitude:  2.5,
									Longitude: 3.5,
								},
								{
									Latitude:  3.5,
									Longitude: 4.5,
									Distance:  ptr(float64(20)),
								},
							},
						},
					},
				},
				Shapes: []Shape{
					{
						ID: "shape_1",
						Points: []ShapePoint{
							{
								Latitude:  1.5,
								Longitude: 2.5,
								Distance:  ptr(float64(0)),
							},
							{
								Latitude:  2.5,
								Longitude: 3.5,
							},
							{
								Latitude:  3.5,
								Longitude: 4.5,
								Distance:  ptr(float64(20)),
							},
						},
					},
				},
			},
		},
		{
			desc: "empty frequencies",
			content: newZipBuilderWithDefaults().add(
				"frequencies.txt",
				"trip_id,start_time,end_time,headway_secs",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes:   []Route{defaultRoute},
				Services: []Service{defaultService},
				Stops:    []Stop{defaultStop},
				Trips:    []ScheduledTrip{defaultTrip},
			},
		},
		{
			desc: "frequencies",
			content: newZipBuilderWithDefaults().add(
				"frequencies.txt",
				"trip_id,start_time,end_time,headway_secs,exact_times",
				"trip_1,00:00:00,01:00:00,180,1",
				"trip_2,01:00:00,02:00:00,300,0",
			).add(
				"trips.txt",
				"route_id,service_id,trip_id",
				"route_id,service_id,trip_1",
				"route_id,service_id,trip_2",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes:   []Route{defaultRoute},
				Services: []Service{defaultService},
				Stops:    []Stop{defaultStop},
				Trips: []ScheduledTrip{
					{
						ID:      "trip_1",
						Route:   &defaultRoute,
						Service: &defaultService,
						Frequencies: []Frequency{
							{
								StartTime:  0 * time.Hour,
								EndTime:    1 * time.Hour,
								Headway:    3 * time.Minute,
								ExactTimes: ScheduleBased,
							},
						},
					},
					{
						ID:      "trip_2",
						Route:   &defaultRoute,
						Service: &defaultService,
						Frequencies: []Frequency{
							{
								StartTime:  1 * time.Hour,
								EndTime:    2 * time.Hour,
								Headway:    5 * time.Minute,
								ExactTimes: FrequencyBased,
							},
						},
					},
				},
			},
		},
		{
			desc: "frequencies without exact times",
			content: newZipBuilderWithDefaults().add(
				"frequencies.txt",
				"trip_id,start_time,end_time,headway_secs",
				"trip_id,00:00:00,01:00:00,180",
			).add(
				"trips.txt",
				"route_id,service_id,trip_id",
				"route_id,service_id,trip_id",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes:   []Route{defaultRoute},
				Services: []Service{defaultService},
				Stops:    []Stop{defaultStop},
				Trips: []ScheduledTrip{
					{
						ID:      defaultTrip.ID,
						Route:   &defaultRoute,
						Service: &defaultService,
						Frequencies: []Frequency{
							{
								StartTime:  0 * time.Hour,
								EndTime:    1 * time.Hour,
								Headway:    3 * time.Minute,
								ExactTimes: FrequencyBased,
							},
						},
					},
				},
			},
		},
		{
			desc: "frequencies with blank exact times",
			content: newZipBuilderWithDefaults().add(
				"frequencies.txt",
				"trip_id,start_time,end_time,headway_secs,exact_times",
				"trip_id,00:00:00,01:00:00,180,",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes:   []Route{defaultRoute},
				Services: []Service{defaultService},
				Stops:    []Stop{defaultStop},
				Trips: []ScheduledTrip{
					{
						ID:      defaultTrip.ID,
						Route:   &defaultRoute,
						Service: &defaultService,
						Frequencies: []Frequency{
							{
								StartTime:  0 * time.Hour,
								EndTime:    1 * time.Hour,
								Headway:    3 * time.Minute,
								ExactTimes: FrequencyBased,
							},
						},
					},
				},
			},
		},
		{
			desc: "frequencies with missing trip",
			content: newZipBuilderWithDefaults().add(
				"frequencies.txt",
				"trip_id,start_time,end_time,headway_secs",
				"some_trip,00:00:00,01:00:00,180",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes:   []Route{defaultRoute},
				Services: []Service{defaultService},
				Stops:    []Stop{defaultStop},
				Trips:    []ScheduledTrip{defaultTrip},
			},
		},
		{
			desc: "trip with multiple frequencies",
			content: newZipBuilderWithDefaults().add(
				"frequencies.txt",
				"trip_id,start_time,end_time,headway_secs,exact_times",
				"trip_id,00:00:00,01:00:00,180,1",
				"trip_id,01:00:00,02:00:00,300,0",
			).build(),
			expected: &Static{
				Agencies: []Agency{defaultAgency},
				Routes:   []Route{defaultRoute},
				Services: []Service{defaultService},
				Stops:    []Stop{defaultStop},
				Trips: []ScheduledTrip{
					{
						ID:      defaultTrip.ID,
						Route:   &defaultRoute,
						Service: &defaultService,
						Frequencies: []Frequency{
							{
								StartTime:  0 * time.Hour,
								EndTime:    1 * time.Hour,
								Headway:    3 * time.Minute,
								ExactTimes: ScheduleBased,
							},
							{
								StartTime:  1 * time.Hour,
								EndTime:    2 * time.Hour,
								Headway:    5 * time.Minute,
								ExactTimes: FrequencyBased,
							},
						},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actual, err := ParseStatic(tc.content, tc.opts)
			if err != nil {
				t.Errorf("error when parsing: %s", err)
			}
			if diff := cmp.Diff(actual, tc.expected); diff != "" {
				t.Errorf("not the same: \n%+v != \n%+v\ndiff:%s", actual, tc.expected, diff)
			}
		})
	}
}

type zipBuilder struct {
	m map[string]string
}

func newZipBuilder() *zipBuilder {
	return (&zipBuilder{m: map[string]string{}}).add(
		"agency.txt", "agency_id,agency_name,agency_url,agency_timezone",
	).add(
		"routes.txt", "route_id,route_type",
	).add(
		"stops.txt", "stop_id",
	).add(
		"transfers.txt", "from_stop_id,to_stop_id",
	).add(
		"trips.txt", "route_id,service_id,trip_id",
	).add(
		"stop_times.txt", "stop_id,trip_id,stop_sequence",
	)
}

func newZipBuilderWithDefaults() *zipBuilder {
	return newZipBuilder().add(
		"agency.txt",
		"agency_id,agency_name,agency_url,agency_timezone\na,b,c,d",
	).add(
		"routes.txt",
		"route_id,route_type\nroute_id,3",
	).add(
		"stops.txt",
		"stop_id\nstop_id",
	).add(
		"calendar.txt",
		"service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"+
			"service_id,0,0,0,0,0,0,0,20220504,20220507",
	).add(
		"stop_times.txt",
		"stop_id,trip_id,arrival_time,departure_time,stop_sequence,stop_headsign\n"+
			"stop_id,a,04:05:06,13:14:15,50,b",
	).add(
		"trips.txt",
		"route_id,service_id,trip_id\nroute_id,service_id,trip_id")
}

func (z *zipBuilder) add(fileName string, fileContent ...string) *zipBuilder {
	z.m[fileName] = strings.Join(fileContent, "\n")
	return z
}

func (z *zipBuilder) build() []byte {
	var b bytes.Buffer
	zipWriter := zip.NewWriter(&b)
	for fileName, fileContent := range z.m {
		fileWriter, err := zipWriter.Create(fileName)
		if err != nil {
			panic(err)
		}
		if _, err := io.Copy(fileWriter, bytes.NewBufferString(fileContent)); err != nil {
			panic(err)
		}
	}
	if err := zipWriter.Close(); err != nil {
		panic(err)
	}
	return b.Bytes()
}

func ptr[T any](t T) *T {
	return &t
}
