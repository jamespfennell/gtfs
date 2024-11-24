package gtfs_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jamespfennell/gtfs"
	"github.com/jamespfennell/gtfs/internal/testutil"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
)

const (
	tripID1    = "tripID1"
	vehicleID1 = "vehicleID1"
	stopID1    = "stopID1"
)

var createTime time.Time = time.Unix(2<<28, 0).UTC()
var time1 time.Time = time.Unix(2<<28+100, 0).UTC()
var time2 time.Time = time.Unix(2<<28+200, 0).UTC()

func TestRealtime(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []*gtfsrt.FeedEntity
		want *gtfs.Realtime
	}{
		{
			name: "trip",
			in: []*gtfsrt.FeedEntity{
				{
					Id: ptr("1"),
					TripUpdate: &gtfsrt.TripUpdate{
						Trip: &gtfsrt.TripDescriptor{
							TripId: ptr(tripID1),
						},
						Vehicle: &gtfsrt.VehicleDescriptor{
							Id: ptr(vehicleID1),
						},
						// TODO: other fields
					},
				},
			},
			want: func() *gtfs.Realtime {
				trip := gtfs.Trip{
					ID: gtfs.TripID{
						ID:          tripID1,
						DirectionID: gtfs.DirectionID_Unspecified,
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
				return &gtfs.Realtime{
					CreatedAt: createTime,
					Trips:     []gtfs.Trip{trip},
					Vehicles:  []gtfs.Vehicle{vehicle},
				}
			}(),
		},
		{
			name: "vehicle",
			in: []*gtfsrt.FeedEntity{
				{
					Id: ptr("1"),
					Vehicle: &gtfsrt.VehiclePosition{
						Trip: &gtfsrt.TripDescriptor{
							TripId: ptr(tripID1),
						},
						Vehicle: &gtfsrt.VehicleDescriptor{
							Id: ptr(vehicleID1),
						},
						Position: &gtfsrt.Position{
							Latitude:  ptr(float32(1.0)),
							Longitude: ptr(float32(2.0)),
							Bearing:   ptr(float32(3.0)),
							Odometer:  ptr(4.0),
							Speed:     ptr(float32(5.0)),
						},
						CurrentStopSequence: ptr(uint32(6)),
						StopId:              ptr(stopID1),
						CurrentStatus:       ptr(gtfsrt.VehiclePosition_STOPPED_AT),
						Timestamp:           ptr(uint64(time1.Unix())),
						CongestionLevel:     ptr(gtfsrt.VehiclePosition_CONGESTION),
						OccupancyStatus:     ptr(gtfsrt.VehiclePosition_EMPTY),
					},
				},
			},
			want: func() *gtfs.Realtime {
				trip := gtfs.Trip{
					ID: gtfs.TripID{
						ID:          tripID1,
						DirectionID: gtfs.DirectionID_Unspecified,
					},
					IsEntityInMessage: false,
				}
				position := gtfs.Position{
					Latitude:  ptr(float32(1.0)),
					Longitude: ptr(float32(2.0)),
					Bearing:   ptr(float32(3.0)),
					Odometer:  ptr(4.0),
					Speed:     ptr(float32(5.0)),
				}
				vehicle := gtfs.Vehicle{
					ID: &gtfs.VehicleID{
						ID: vehicleID1,
					},
					Position:            &position,
					CurrentStopSequence: ptr(uint32(6)),
					StopID:              ptr(stopID1),
					CurrentStatus:       ptr(gtfsrt.VehiclePosition_STOPPED_AT),
					Timestamp:           &time1,
					CongestionLevel:     gtfsrt.VehiclePosition_CONGESTION,
					OccupancyStatus:     ptr(gtfsrt.VehiclePosition_EMPTY),
					IsEntityInMessage:   true,
				}
				trip.Vehicle = &vehicle
				vehicle.Trip = &trip

				return &gtfs.Realtime{
					CreatedAt: createTime,
					Trips:     []gtfs.Trip{trip},
					Vehicles:  []gtfs.Vehicle{vehicle},
				}
			}(),
		},
		{
			name: "alert",
			in: []*gtfsrt.FeedEntity{
				{
					Id: ptr("AlertID"),
					Alert: &gtfsrt.Alert{
						ActivePeriod: []*gtfsrt.TimeRange{
							{
								Start: ptr(uint64(time1.Unix())),
								End:   ptr(uint64(time2.Unix())),
							},
						},
						InformedEntity: []*gtfsrt.EntitySelector{
							{
								AgencyId: ptr("AgencyID"),
							},
							{
								RouteId:   ptr("RouteID"),
								RouteType: ptr(int32(gtfs.RouteType_Subway)),
							},
							{
								StopId: ptr("StopID"),
							},
							{
								Trip: &gtfsrt.TripDescriptor{
									TripId: ptr("TripID"),
								},
								DirectionId: parseDirectionID_GTFSRealtimeRaw(gtfs.DirectionID_True),
							},
						},
						Cause:  ptr(gtfsrt.Alert_CONSTRUCTION),
						Effect: ptr(gtfsrt.Alert_SIGNIFICANT_DELAYS),
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
			},
			want: &gtfs.Realtime{
				CreatedAt: createTime,
				Alerts: []gtfs.Alert{
					{
						ID: "AlertID",
						ActivePeriods: []gtfs.AlertActivePeriod{
							{
								StartsAt: ptr(time1),
								EndsAt:   ptr(time2),
							},
						},
						InformedEntities: []gtfs.AlertInformedEntity{
							{
								AgencyID:  ptr("AgencyID"),
								RouteType: gtfs.RouteType_Unknown,
							},
							{
								RouteID:   ptr("RouteID"),
								RouteType: gtfs.RouteType_Subway,
							},
							{
								StopID:    ptr("StopID"),
								RouteType: gtfs.RouteType_Unknown,
							},
							{
								TripID: &gtfs.TripID{
									ID:          "TripID",
									RouteID:     "",
									DirectionID: gtfs.DirectionID_Unspecified,
								},
								RouteType:   gtfs.RouteType_Unknown,
								DirectionID: gtfs.DirectionID_True,
							},
						},
						Cause:  gtfsrt.Alert_CONSTRUCTION,
						Effect: gtfsrt.Alert_SIGNIFICANT_DELAYS,
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
					},
				},
				Trips: []gtfs.Trip{
					{
						ID: gtfs.TripID{
							ID: "TripID",
						},
						IsEntityInMessage: false,
					},
				},
			},
		},
		{
			name: "trip and vehicle",
			in: []*gtfsrt.FeedEntity{
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
				{
					Id: ptr("2"),
					Vehicle: &gtfsrt.VehiclePosition{
						Trip: &gtfsrt.TripDescriptor{
							TripId: ptr(tripID1),
						},
						Vehicle: &gtfsrt.VehicleDescriptor{
							Id: ptr(vehicleID1),
						},
					},
				},
			},
			want: func() *gtfs.Realtime {
				trip := gtfs.Trip{
					ID: gtfs.TripID{
						ID:          tripID1,
						DirectionID: gtfs.DirectionID_Unspecified,
					},
					IsEntityInMessage: true,
				}
				vehicle := gtfs.Vehicle{
					ID: &gtfs.VehicleID{
						ID: vehicleID1,
					},
					IsEntityInMessage: true,
				}
				trip.Vehicle = &vehicle
				vehicle.Trip = &trip
				return &gtfs.Realtime{
					CreatedAt: createTime,
					Trips:     []gtfs.Trip{trip},
					Vehicles:  []gtfs.Vehicle{vehicle},
				}
			}(),
		},
		{
			name: "alert trip descriptor with empty trip id, route id, and unspecified direction id",
			in: []*gtfsrt.FeedEntity{
				{
					Id: ptr("AlertID"),
					Alert: func() *gtfsrt.Alert {
						alert := buildBaseRtAlert()
						alert.InformedEntity = []*gtfsrt.EntitySelector{
							{
								Trip: &gtfsrt.TripDescriptor{
									RouteId: ptr("RouteID"),
								},
							},
						}

						return alert
					}(),
				},
			},
			want: &gtfs.Realtime{
				CreatedAt: createTime,
				Alerts: []gtfs.Alert{
					{
						ID: "AlertID",
						ActivePeriods: []gtfs.AlertActivePeriod{
							{
								StartsAt: ptr(time1),
								EndsAt:   ptr(time2),
							},
						},
						InformedEntities: []gtfs.AlertInformedEntity{
							{
								RouteID:   ptr("RouteID"),
								RouteType: gtfs.RouteType_Unknown,
							},
						},
						Cause:  gtfsrt.Alert_CONSTRUCTION,
						Effect: gtfsrt.Alert_SIGNIFICANT_DELAYS,
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
					},
				},
			},
		},
		{
			name: "alert trip descriptor with empty trip id, route id, and both direction ids",
			in: []*gtfsrt.FeedEntity{
				{
					Id: ptr("AlertID"),
					Alert: func() *gtfsrt.Alert {
						alert := buildBaseRtAlert()
						alert.InformedEntity = []*gtfsrt.EntitySelector{
							{
								Trip: &gtfsrt.TripDescriptor{
									RouteId:     ptr("RouteID"),
									DirectionId: parseDirectionID_GTFSRealtimeRaw(gtfs.DirectionID_True),
								},
							},
							{
								Trip: &gtfsrt.TripDescriptor{
									RouteId:     ptr("RouteID"),
									DirectionId: parseDirectionID_GTFSRealtimeRaw(gtfs.DirectionID_False),
								},
							},
						}

						return alert
					}(),
				},
			},
			want: &gtfs.Realtime{
				CreatedAt: createTime,
				Alerts: []gtfs.Alert{
					{
						ID: "AlertID",
						ActivePeriods: []gtfs.AlertActivePeriod{
							{
								StartsAt: ptr(time1),
								EndsAt:   ptr(time2),
							},
						},
						InformedEntities: []gtfs.AlertInformedEntity{
							{
								RouteID:   ptr("RouteID"),
								RouteType: gtfs.RouteType_Unknown,
							},
						},
						Cause:  gtfsrt.Alert_CONSTRUCTION,
						Effect: gtfsrt.Alert_SIGNIFICANT_DELAYS,
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
					},
				},
			},
		},
		{
			name: "alert trip descriptor with empty trip id, route id, and single direction id",
			in: []*gtfsrt.FeedEntity{
				{
					Id: ptr("AlertID"),
					Alert: func() *gtfsrt.Alert {
						alert := buildBaseRtAlert()
						alert.InformedEntity = []*gtfsrt.EntitySelector{
							{
								Trip: &gtfsrt.TripDescriptor{
									RouteId:     ptr("RouteID"),
									DirectionId: parseDirectionID_GTFSRealtimeRaw(gtfs.DirectionID_True),
								},
							},
						}

						return alert
					}(),
				},
			},
			want: &gtfs.Realtime{
				CreatedAt: createTime,
				Alerts: []gtfs.Alert{
					{
						ID: "AlertID",
						ActivePeriods: []gtfs.AlertActivePeriod{
							{
								StartsAt: ptr(time1),
								EndsAt:   ptr(time2),
							},
						},
						InformedEntities: []gtfs.AlertInformedEntity{
							{
								RouteID:     ptr("RouteID"),
								RouteType:   gtfs.RouteType_Unknown,
								DirectionID: gtfs.DirectionID_True,
							},
						},
						Cause:  gtfsrt.Alert_CONSTRUCTION,
						Effect: gtfsrt.Alert_SIGNIFICANT_DELAYS,
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
					},
				},
			},
		},
		{
			name: "alert trip descriptor with empty trip id and route id doesn't overwrite normal informed route",
			in: []*gtfsrt.FeedEntity{
				{
					Id: ptr("AlertID"),
					Alert: func() *gtfsrt.Alert {
						alert := buildBaseRtAlert()
						alert.InformedEntity = []*gtfsrt.EntitySelector{
							{
								Trip: &gtfsrt.TripDescriptor{
									RouteId: ptr("RouteID"),
								},
							},
							{
								RouteId:   ptr("RouteID"),
								RouteType: ptr(int32(gtfs.RouteType_Subway)),
							},
						}

						return alert
					}(),
				},
			},
			want: &gtfs.Realtime{
				CreatedAt: createTime,
				Alerts: []gtfs.Alert{
					{
						ID: "AlertID",
						ActivePeriods: []gtfs.AlertActivePeriod{
							{
								StartsAt: ptr(time1),
								EndsAt:   ptr(time2),
							},
						},
						InformedEntities: []gtfs.AlertInformedEntity{
							{
								RouteID:   ptr("RouteID"),
								RouteType: gtfs.RouteType_Subway,
							},
						},
						Cause:  gtfsrt.Alert_CONSTRUCTION,
						Effect: gtfsrt.Alert_SIGNIFICANT_DELAYS,
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
					},
				},
			},
		},
		{
			name: "trip informed entity with empty trip id but still identifiable",
			in: []*gtfsrt.FeedEntity{
				{
					Id: ptr("AlertID"),
					Alert: func() *gtfsrt.Alert {
						alert := buildBaseRtAlert()
						alert.InformedEntity = []*gtfsrt.EntitySelector{
							{
								Trip: &gtfsrt.TripDescriptor{
									RouteId:     ptr("RouteID"),
									DirectionId: parseDirectionID_GTFSRealtimeRaw(gtfs.DirectionID_True),
									StartDate:   ptr("20240101"),
									StartTime:   ptr("11:00:00"),
								},
							},
						}
						return alert
					}(),
				},
			},
			want: &gtfs.Realtime{
				CreatedAt: createTime,
				Alerts: []gtfs.Alert{
					{
						ID: "AlertID",
						ActivePeriods: []gtfs.AlertActivePeriod{
							{
								StartsAt: ptr(time1),
								EndsAt:   ptr(time2),
							},
						},
						InformedEntities: []gtfs.AlertInformedEntity{
							{
								TripID: &gtfs.TripID{
									RouteID:      "RouteID",
									DirectionID:  gtfs.DirectionID_True,
									HasStartDate: true,
									StartDate:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
									HasStartTime: true,
									StartTime:    11 * time.Hour,
								},
								RouteType: gtfs.RouteType_Unknown,
							},
						},
						Cause:  gtfsrt.Alert_CONSTRUCTION,
						Effect: gtfsrt.Alert_SIGNIFICANT_DELAYS,
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
					},
				},
				Trips: []gtfs.Trip{
					{
						ID: gtfs.TripID{
							ID:           "",
							RouteID:      "RouteID",
							DirectionID:  gtfs.DirectionID_True,
							HasStartDate: true,
							StartDate:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
							HasStartTime: true,
							StartTime:    11 * time.Hour,
						},
					},
				},
			},
		},
		{
			name: "useless informed entities are ignored",
			in: []*gtfsrt.FeedEntity{
				{
					Id: ptr("AlertID"),
					Alert: func() *gtfsrt.Alert {
						alert := buildBaseRtAlert()
						alert.InformedEntity = []*gtfsrt.EntitySelector{
							// Empty
							{},
							{
								// No route ID
								Trip: &gtfsrt.TripDescriptor{
									DirectionId: parseDirectionID_GTFSRealtimeRaw(gtfs.DirectionID_True),
									StartDate:   ptr("20240101"),
									StartTime:   ptr("11:00:00"),
								},
							},
							{
								// Direction ID without a route
								DirectionId: parseDirectionID_GTFSRealtimeRaw(gtfs.DirectionID_True),
							},
						}
						return alert
					}(),
				},
			},
			want: &gtfs.Realtime{
				CreatedAt: createTime,
				Alerts: []gtfs.Alert{
					{
						ID: "AlertID",
						ActivePeriods: []gtfs.AlertActivePeriod{
							{
								StartsAt: ptr(time1),
								EndsAt:   ptr(time2),
							},
						},
						Cause:  gtfsrt.Alert_CONSTRUCTION,
						Effect: gtfsrt.Alert_SIGNIFICANT_DELAYS,
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
					},
				},
			},
		},
		{
			name: "trip id is removed from informed entity when not uniquely identifiable",
			in: []*gtfsrt.FeedEntity{
				{
					Id: ptr("AlertID"),
					Alert: func() *gtfsrt.Alert {
						alert := buildBaseRtAlert()
						alert.InformedEntity = []*gtfsrt.EntitySelector{
							{
								RouteId: ptr("RouteID"),
								// No route ID
								Trip: &gtfsrt.TripDescriptor{
									DirectionId: parseDirectionID_GTFSRealtimeRaw(gtfs.DirectionID_True),
									StartDate:   ptr("20240101"),
									StartTime:   ptr("11:00:00"),
								},
							},
						}
						return alert
					}(),
				},
			},
			want: &gtfs.Realtime{
				CreatedAt: createTime,
				Alerts: []gtfs.Alert{
					{
						ID: "AlertID",
						ActivePeriods: []gtfs.AlertActivePeriod{
							{
								StartsAt: ptr(time1),
								EndsAt:   ptr(time2),
							},
						},
						InformedEntities: []gtfs.AlertInformedEntity{
							{
								RouteID:   ptr("RouteID"),
								RouteType: gtfs.RouteType_Unknown,
							},
						},
						Cause:  gtfsrt.Alert_CONSTRUCTION,
						Effect: gtfsrt.Alert_SIGNIFICANT_DELAYS,
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
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header := &gtfsrt.FeedHeader{
				GtfsRealtimeVersion: ptr("2.0"),
				Timestamp:           ptr(uint64(createTime.Unix())),
			}
			got := testutil.MustParse(t, header, tc.in, &gtfs.ParseRealtimeOptions{})

			if diff := cmp.Diff(got, tc.want); diff != "" {
				t.Errorf("got:\n%+v\n!= want:\n%+v\ndiff: %s", got, tc.want, diff)
			}
		})
	}
}

func buildBaseRtAlert() *gtfsrt.Alert {
	return &gtfsrt.Alert{
		ActivePeriod: []*gtfsrt.TimeRange{
			{
				Start: ptr(uint64(time1.Unix())),
				End:   ptr(uint64(time2.Unix())),
			},
		},
		Cause:  ptr(gtfsrt.Alert_CONSTRUCTION),
		Effect: ptr(gtfsrt.Alert_SIGNIFICANT_DELAYS),
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
	}
}

func parseDirectionID_GTFSRealtimeRaw(d gtfs.DirectionID) *uint32 {
	if d == gtfs.DirectionID_Unspecified {
		return nil
	}
	if d == gtfs.DirectionID_False {
		return ptr(uint32(0))
	}
	return ptr(uint32(1))
}

func ptr[T any](t T) *T {
	return &t
}
