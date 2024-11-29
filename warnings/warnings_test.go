package warnings

// Verify that StaticWarningKind satisfies the error interface.
var (
	w StaticWarningKind = nil
	e error             = w
)
