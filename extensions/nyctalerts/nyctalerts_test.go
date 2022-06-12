package nyctalerts_test

import (
	"reflect"
	"testing"

	"github.com/jamespfennell/gtfs"
	"github.com/jamespfennell/gtfs/extensions/nyctalerts"
	gtfsrt "github.com/jamespfennell/gtfs/proto"
	"google.golang.org/protobuf/proto"
)

func TestElevatorAlerts(t *testing.T) {
	createOutAlert := func(alertID string, informedStopID ...string) gtfs.Alert {
		var informedEntites []gtfs.AlertInformedEntity
		for _, stopID := range informedStopID {
			stopID := stopID
			informedEntites = append(informedEntites, gtfs.AlertInformedEntity{
				StopID: &stopID,
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
			name: "no deduplication",
			opts: nyctalerts.ExtensionOpts{
				DeduplicateElevatorAlerts:      false,
				UseStationIDsForElevatorAlerts: false,
			},
			wantAlerts: []gtfs.Alert{
				createOutAlert("R25N#EL728", "R25N"),
				createOutAlert("R25S#EL728", "R25S"),
				createOutAlert("E01N#EL728", "E01N"),
				createOutAlert("E01S#EL728", "E01S"),
			},
		},
		{
			name: "platform ID to station ID",
			opts: nyctalerts.ExtensionOpts{
				DeduplicateElevatorAlerts:      false,
				UseStationIDsForElevatorAlerts: true,
			},
			wantAlerts: []gtfs.Alert{
				createOutAlert("R25#EL728", "R25"),
				createOutAlert("E01#EL728", "E01"),
			},
		},
		{
			name: "deduplicate with platform ID",
			opts: nyctalerts.ExtensionOpts{
				DeduplicateElevatorAlerts:      true,
				UseStationIDsForElevatorAlerts: false,
			},
			wantAlerts: []gtfs.Alert{
				createOutAlert("elevator:EL728", "R25N", "R25S", "E01N", "E01S"),
			},
		},
		{
			name: "deduplicate with station ID",
			opts: nyctalerts.ExtensionOpts{
				DeduplicateElevatorAlerts:      true,
				UseStationIDsForElevatorAlerts: true,
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
			message := gtfsrt.FeedMessage{
				Header: &gtfsrt.FeedHeader{
					GtfsRealtimeVersion: ptr("2.0"),
				},
				Entity: alerts,
			}
			b, err := proto.Marshal(&message)
			if err != nil {
				t.Fatalf("Failed to marshal message: %s", err)
			}
			_ = b

			result, err := gtfs.ParseRealtime(b, &gtfs.ParseRealtimeOptions{
				Extension: nyctalerts.Extension(tc.opts),
			})
			if err != nil {
				t.Errorf("unexpected error in ParseRealtime: %s", err)
			}
			if !reflect.DeepEqual(result.Alerts, tc.wantAlerts) {
				t.Errorf("got != want\n got=%+v\nwant=%+v", result.Alerts, tc.wantAlerts)
			}
		})
	}
}

func ptr(s string) *string {
	return &s
}
