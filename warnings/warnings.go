package warnings

import (
	"fmt"

	"github.com/jamespfennell/gtfs/constants"
	"github.com/jamespfennell/gtfs/csv"
)

type StaticWarning struct {
	// Kind of warning
	Kind StaticWarningKind
	// File in the warning comes from
	File constants.StaticFile
	// Row number of the problematic row
	//
	// This is not necessarily the line number in the file.
	// For example, if the CSV file contains empty lines these won't be
	// counted towards the row number.
	RowNumber int
	// Content of the problematic row
	RowContent []string
	// Content of the header of the csv file.
	// If the warning is for the header this will be identical to LineContent.
	HeaderContent []string
}

func NewStaticWarning(csvFile *csv.File, kind StaticWarningKind) StaticWarning {
	return StaticWarning{
		Kind:          kind,
		File:          csvFile.Name(),
		RowNumber:     csvFile.RowNumber(),
		RowContent:    csvFile.RowContent(),
		HeaderContent: csvFile.HeaderContent(),
	}
}

// StaticWarningKind represents the kind of warning raised during GTFS static parsing.
//
// StaticWarningKind satisfies the error interface.
type StaticWarningKind interface {
	// Text of the warning message.
	Error() string // TODO: Message()

	// TODO: Fatal() ? And convert all parsing errors into warnings
}

type MissingColumns struct {
	Columns []string
}

func (w MissingColumns) Error() string {
	return fmt.Sprintf("csv file is missing columns %s", w.Columns)
}

type MissingConditionallyRequiredColumn struct {
	Column    string
	Condition string
}

func (w MissingConditionallyRequiredColumn) Error() string {
	return fmt.Sprintf("column %q is required when %s", w.Column, w.Condition)
}

type RowMissingRequiredValue struct {
	Entity constants.ScheduleEnity
	Id     string
	Column string
}

func (w RowMissingRequiredValue) Error() string {
	return fmt.Sprintf("%s %q is missing required value for column %q", w.Entity, w.Id, w.Column)
}

type RowMissingConditionallyRequiredValue struct {
	Entity    constants.ScheduleEnity
	Id        string
	Column    string
	Condition string
}

func (w RowMissingConditionallyRequiredValue) Error() string {
	return fmt.Sprintf("%s %q is missing required value for column %q when %s", w.Entity, w.Id, w.Column, w.Condition)
}

type RowInvalidValue struct {
	Entity constants.ScheduleEnity
	Id     string
	Column string
	Value  string
	Reason string
}

func (w RowInvalidValue) Error() string {
	if w.Reason == "" {
		return fmt.Sprintf("%s %q has invalid value %s for column %q", w.Entity, w.Id, w.Value, w.Column)
	}
	return fmt.Sprintf("%s %q has invalid value %s for column %q: %s", w.Entity, w.Id, w.Value, w.Column, w.Reason)
}

type RowInvalidForeignKey struct {
	Entity          constants.ScheduleEnity
	ReferenceEntity constants.ScheduleEnity
	Id              string
	Column          string
	Value           string
}

func (w RowInvalidForeignKey) Error() string {
	return fmt.Sprintf("%s %q has invalid foreign key %s reference to %s for column %q", w.Entity, w.Id, w.Value, w.ReferenceEntity, w.Column)
}

type RowInvalidForeignId struct {
	Entity          constants.ScheduleEnity
	ReferenceEntity constants.ScheduleEnity
	ReferenceId     constants.ForeignId
	Id              string
	Column          string
	Value           string
}

func (w RowInvalidForeignId) Error() string {
	return fmt.Sprintf("%s %q has invalid foreign key %s reference to %s %q for column %q", w.Entity, w.Id, w.Value, w.ReferenceEntity, w.ReferenceId, w.Column)
}

type AgencyMissingValues struct {
	AgencyID string
	Columns  []string
}

func (w AgencyMissingValues) Error() string {
	return fmt.Sprintf("agency %q is missing values %s", w.AgencyID, w.Columns)
}
