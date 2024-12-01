package journal

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"
	"time"

	"github.com/jamespfennell/gtfs"
)

//go:embed trips.csv.tmpl
var tripsCsvTmpl string

//go:embed stop_times.csv.tmpl
var stopTimesCsvTmpl string

var funcMap = template.FuncMap{
	"NullableString": func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	},
	"NullableUnix": func(t *time.Time) string {
		if t == nil {
			return ""
		}
		return fmt.Sprintf("%d", t.Unix())
	},
	"FormatDirectionID": func(d gtfs.DirectionID) string {
		switch d {
		case gtfs.DirectionID_False:
			return "0"
		case gtfs.DirectionID_True:
			return "1"
		default:
			return ""
		}
	},
}

var tripsCsv *template.Template = template.Must(template.New("trips.csv.tmpl").Funcs(funcMap).Parse(tripsCsvTmpl))
var stopTimesCsv *template.Template = template.Must(template.New("stop_times.csv.tmpl").Funcs(funcMap).Parse(stopTimesCsvTmpl))

// CsvExport contains CSV exports of a journal
type CsvExport struct {
	TripsCsv     []byte
	StopTimesCsv []byte
}

func (journal *Journal) ExportToCsv() (*CsvExport, error) {
	var tripsB bytes.Buffer
	err := tripsCsv.Execute(&tripsB, journal.Trips)
	if err != nil {
		return nil, err
	}

	var stopTimesB bytes.Buffer
	err = stopTimesCsv.Execute(&stopTimesB, journal.Trips)
	if err != nil {
		return nil, err
	}
	return &CsvExport{
		TripsCsv:     tripsB.Bytes(),
		StopTimesCsv: stopTimesB.Bytes(),
	}, nil
}
