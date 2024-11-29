package warnings

import (
	"fmt"

	"github.com/jamespfennell/gtfs/constants"
)

// StaticWarning represents a warning raised during GTFS static parsing.
//
// StaticWarning satisfies the error interface.
type StaticWarning interface {
	// The file the warning originates from.
	File() constants.StaticFile
	// Text of the warning message.
	Error() string
}

type MissingColumns struct {
	File_   constants.StaticFile
	Columns []string
}

func (w MissingColumns) File() constants.StaticFile {
	return w.File_
}

func (w MissingColumns) Error() string {
	return fmt.Sprintf("skipping file because of missing columns %s", w.Columns)
}

type AgencyMissingColumns struct {
	AgencyID string
	Columns  []string
}

func (w AgencyMissingColumns) File() constants.StaticFile {
	return constants.AgencyFile
}

func (w AgencyMissingColumns) Error() string {
	return fmt.Sprintf("skipping agency %q because of missing columns %s", w.AgencyID, w.Columns)
}
