package warnings

import (
	"fmt"

	"github.com/jamespfennell/gtfs/constants"
)

type StaticWarning interface {
	File() constants.StaticFile
	Error() string
}

type AgencyMissingColumns struct {
	AgencyID    string
	MissingKeys []string
}

func (w AgencyMissingColumns) File() constants.StaticFile {
	return constants.AgencyFile
}

func (w AgencyMissingColumns) Error() string {
	return fmt.Sprintf("skipping agency %q because of missing columns %s", w.AgencyID, w.MissingKeys)
}
