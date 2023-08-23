package nyctalerts_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/jamespfennell/gtfs"
	"github.com/jamespfennell/gtfs/extensions/nyctalerts"
	"github.com/jamespfennell/gtfs/internal/testutil"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
	"google.golang.org/protobuf/proto"
)

func TestMetadata(t *testing.T) {
	createdAt := uint64(400*24*60*60 + 100)
	updatedAt := uint64(400*24*60*60 + 200)
	displayBeforeActive := uint64(300)
	activePeriod := "Every Monday"
	nyctAlert := gtfsrt.MercuryAlert{
		CreatedAt:           &createdAt,
		UpdatedAt:           &updatedAt,
		AlertType:           ptr(""),
		DisplayBeforeActive: &displayBeforeActive,
		HumanReadableActivePeriod: &gtfsrt.TranslatedString{
			Translation: []*gtfsrt.TranslatedString_Translation{
				{
					Text: &activePeriod,
				},
			},
		},
	}
	alert := gtfsrt.Alert{}
	proto.SetExtension(&alert, gtfsrt.E_MercuryAlert, &nyctAlert)
	entities := []*gtfsrt.FeedEntity{
		{
			Id:    ptr("lmm:planned_work"),
			Alert: &alert,
		},
	}

	wantMetadata := nyctalerts.Metadata{
		CreatedAt:                 time.Unix(int64(createdAt), 0),
		UpdatedAt:                 time.Unix(int64(updatedAt), 0),
		DisplayBeforeActive:       300 * time.Second,
		HumanReadableActivePeriod: activePeriod,
	}
	b, err := json.Marshal(&wantMetadata)
	if err != nil {
		t.Fatalf("failed to marshal metadata: %s", err)
	}
	wantAlert := gtfs.Alert{
		ID:     "lmm:planned_work",
		Cause:  gtfs.Maintenance,
		Effect: gtfs.UnknownEffect,
		Description: []gtfs.AlertText{
			{
				Text:     string(b),
				Language: nyctalerts.MetadataLanguage,
			},
		},
	}

	result := testutil.MustParse(t, nil, entities, &gtfs.ParseRealtimeOptions{
		Extension: nyctalerts.Extension(nyctalerts.ExtensionOpts{
			AddNyctMetadata: true,
		}),
	})

	if !reflect.DeepEqual(result.Alerts, []gtfs.Alert{wantAlert}) {
		t.Errorf(" got != want\n got = %+v\nwant = %+v", result.Alerts, []gtfs.Alert{wantAlert})
	}
}

func TestElevatorAlerts(t *testing.T) {
	createOutAlert := func(alertID string, informedStopID ...string) gtfs.Alert {
		var informedEntites []gtfs.AlertInformedEntity
		for _, stopID := range informedStopID {
			stopID := stopID
			informedEntites = append(informedEntites, gtfs.AlertInformedEntity{
				StopID:    &stopID,
				RouteType: gtfs.RouteType_Unknown,
			})
		}
		return gtfs.Alert{
			ID:               alertID,
			Cause:            gtfs.Maintenance,
			Effect:           gtfs.AccessibilityIssue,
			InformedEntities: informedEntites,
		}
	}
	for _, tc := range []struct {
		name       string
		opts       nyctalerts.ExtensionOpts
		wantAlerts []gtfs.Alert
	}{
		{
			name: "no deduplication/platform IDs",
			opts: nyctalerts.ExtensionOpts{
				ElevatorAlertsDeduplicationPolicy:   nyctalerts.NoDeduplication,
				ElevatorAlertsInformUsingStationIDs: false,
			},
			wantAlerts: []gtfs.Alert{
				createOutAlert("R25N#EL728", "R25N"),
				createOutAlert("R25S#EL728", "R25S"),
				createOutAlert("E01N#EL728", "E01N"),
				createOutAlert("E01S#EL728", "E01S"),
			},
		},
		{
			name: "no deduplication/station IDs",
			opts: nyctalerts.ExtensionOpts{
				ElevatorAlertsDeduplicationPolicy:   nyctalerts.NoDeduplication,
				ElevatorAlertsInformUsingStationIDs: true,
			},
			wantAlerts: []gtfs.Alert{
				createOutAlert("R25N#EL728", "R25"),
				createOutAlert("R25S#EL728", "R25"),
				createOutAlert("E01N#EL728", "E01"),
				createOutAlert("E01S#EL728", "E01"),
			},
		},
		{
			name: "station deduplication/platform IDs",
			opts: nyctalerts.ExtensionOpts{
				ElevatorAlertsDeduplicationPolicy:   nyctalerts.DeduplicateInStation,
				ElevatorAlertsInformUsingStationIDs: false,
			},
			wantAlerts: []gtfs.Alert{
				createOutAlert("R25#EL728", "R25N", "R25S"),
				createOutAlert("E01#EL728", "E01N", "E01S"),
			},
		},
		{
			name: "station deduplication/station IDs",
			opts: nyctalerts.ExtensionOpts{
				ElevatorAlertsDeduplicationPolicy:   nyctalerts.DeduplicateInStation,
				ElevatorAlertsInformUsingStationIDs: true,
			},
			wantAlerts: []gtfs.Alert{
				createOutAlert("R25#EL728", "R25"),
				createOutAlert("E01#EL728", "E01"),
			},
		},
		{
			name: "complex deduplication/platform IDs",
			opts: nyctalerts.ExtensionOpts{
				ElevatorAlertsDeduplicationPolicy:   nyctalerts.DeduplicateInComplex,
				ElevatorAlertsInformUsingStationIDs: false,
			},
			wantAlerts: []gtfs.Alert{
				createOutAlert("elevator:EL728", "R25N", "R25S", "E01N", "E01S"),
			},
		},
		{
			name: "complex deduplication/station IDs",
			opts: nyctalerts.ExtensionOpts{
				ElevatorAlertsDeduplicationPolicy:   nyctalerts.DeduplicateInComplex,
				ElevatorAlertsInformUsingStationIDs: true,
			},
			wantAlerts: []gtfs.Alert{
				createOutAlert("elevator:EL728", "R25", "E01"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var alerts []*gtfsrt.FeedEntity
			for _, stopID := range []string{"R25N", "R25S", "E01N", "E01S"} {
				stopID := stopID
				alertID := stopID + "#EL728"
				alerts = append(alerts, &gtfsrt.FeedEntity{
					Id: &alertID,
					Alert: &gtfsrt.Alert{
						InformedEntity: []*gtfsrt.EntitySelector{
							{
								StopId: &stopID,
							},
						},
					},
				})
			}

			result := testutil.MustParse(t, nil, alerts, &gtfs.ParseRealtimeOptions{
				Extension: nyctalerts.Extension(tc.opts),
			})

			if !reflect.DeepEqual(result.Alerts, tc.wantAlerts) {
				t.Errorf("got != want\n got=%+v\nwant=%+v", result.Alerts, tc.wantAlerts)
			}
		})
	}
}

func ptr(s string) *string {
	return &s
}
