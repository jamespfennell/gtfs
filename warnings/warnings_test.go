package warnings

// Verify that StaticWarning satisfies the error interface.
var (
	w StaticWarning = nil
	e error         = w
)
