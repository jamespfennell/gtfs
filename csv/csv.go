// Package csv is a wrapper around the stdlib csv library that provides a nice API for the GTFS static parser.
//
// Because, of course, everything can be solved with another layer of indirection.
package csv

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/jamespfennell/gtfs/constants"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

type MissingConditionallyRequiredColumn struct {
	Column    string
	Condition string
}

type File struct {
	name                                constants.StaticFile
	csvReader                           *csv.Reader
	headerMap                           map[string]int
	headerContent                       []string
	rowNumber                           int
	missingRequiredColumns              []string
	missingConditionallyRequiredColumns []MissingConditionallyRequiredColumn
	currentRow                          *row
	ioErr                               error
	closer                              func() error
}

type MissingKeyWithCondition struct {
	key       string
	condition string
}

type row struct {
	cells                    []string
	missingKeys              []string
	missingKeysWithCondition []MissingKeyWithCondition
}

func New(name constants.StaticFile, reader io.ReadCloser) (*File, error) {
	csvReader := BOMAwareCSVReader(reader)
	firstRow, err := csvReader.Read()
	// We don't reuse the first/header record as we keep this around
	// for populating static warnings.
	csvReader.ReuseRecord = true
	if err == io.EOF {
		reader.Close()
		return nil, fmt.Errorf("CSV file contains no rows")
	} else if err != nil {
		reader.Close()
		return nil, err
	}
	m := map[string]int{}
	for i, colHeader := range firstRow {
		m[colHeader] = i
	}
	return &File{
		name:          name,
		headerMap:     m,
		headerContent: firstRow,
		csvReader:     csvReader,
		closer:        reader.Close,
	}, nil
}

func (f *File) Name() constants.StaticFile {
	return f.name
}

func (f *File) HeaderContent() []string {
	return f.headerContent
}

type RequiredColumn struct {
	i          int
	s          string
	f          *File
	allowEmpty bool
}

func (f *File) requiredColumn(s string, allowEmpty bool) RequiredColumn {
	i, b := f.headerMap[s]
	if !b {
		f.missingRequiredColumns = append(f.missingRequiredColumns, s)
		i = -1
	}
	return RequiredColumn{i, s, f, allowEmpty}
}

func (f *File) RequiredColumn(s string) RequiredColumn {
	return f.requiredColumn(s, false)
}

func (f *File) RequiredColumnAllowEmpty(s string) RequiredColumn {
	return f.requiredColumn(s, true)
}

func (p *File) MissingRequiredColumns() []string {
	if len(p.missingRequiredColumns) == 0 {
		return nil
	}
	return p.missingRequiredColumns
}

func (c RequiredColumn) Name() string {
	return c.s
}

func (c RequiredColumn) Read() string {
	r := c.f.currentRow
	if c.i >= len(r.cells) || (!c.allowEmpty && r.cells[c.i] == "") {
		r.missingKeys = append(r.missingKeys, c.s)
		return ""
	}
	return c.f.currentRow.cells[c.i]
}

type ConditionallyRequiredColumn struct {
	i           int
	s           string
	f           *File
	condition   string
	isRequired  bool
	allowsEmpty bool
}

func (f *File) conditionallyRequiredColumn(s string, condition string, isRequired bool, allowEmpty bool) ConditionallyRequiredColumn {
	i, b := f.headerMap[s]
	if !b {
		i = -1
		if isRequired {
			f.missingConditionallyRequiredColumns = append(f.missingConditionallyRequiredColumns,
				MissingConditionallyRequiredColumn{Column: s, Condition: condition})
		}
	}
	return ConditionallyRequiredColumn{i, s, f, condition, isRequired, allowEmpty}
}

func (f *File) ConditionallyRequiredColumn(s string, condition string, isRequred bool) ConditionallyRequiredColumn {
	return f.conditionallyRequiredColumn(s, condition, isRequred, false)
}

func (f *File) ConditionallyRequiredColumnAllowEmpty(s string, condition string, isRequred bool) ConditionallyRequiredColumn {
	return f.conditionallyRequiredColumn(s, condition, isRequred, true)
}

func (p *File) MissingConditionallyRequiredColumns() []MissingConditionallyRequiredColumn {
	if len(p.missingConditionallyRequiredColumns) == 0 {
		return nil
	}
	return p.missingConditionallyRequiredColumns
}

func (c ConditionallyRequiredColumn) Name() string {
	return c.s
}

func (c ConditionallyRequiredColumn) Read() string {
	r := c.f.currentRow
	if c.i < 0 || c.i >= len(r.cells) || (!c.allowsEmpty && r.cells[c.i] == "") {
		if c.isRequired {
			r.missingKeysWithCondition = append(r.missingKeysWithCondition,
				MissingKeyWithCondition{key: c.s, condition: c.condition})
		}
		return ""
	}
	return c.f.currentRow.cells[c.i]
}

type OptionalColumn struct {
	i int
	s string
	f *File
}

func (f *File) OptionalColumn(s string) OptionalColumn {
	i, b := f.headerMap[s]
	if !b {
		i = -1
	}
	return OptionalColumn{i: i, s: s, f: f}
}

func (c OptionalColumn) Name() string {
	return c.s
}

func (c OptionalColumn) Read() string {
	if c.i < 0 {
		return ""
	}
	// We copy the string pointer because the CSV library reuses the byte array across rows.
	s := c.f.currentRow.cells[c.i]
	return s
}

func (c OptionalColumn) ReadOr(s string) string {
	if c.i < 0 {
		return s
	}
	return c.f.currentRow.cells[c.i]
}

func (f *File) NextRow() bool {
	cells, err := f.csvReader.Read()
	if err == io.EOF {
		f.currentRow = nil
		return false
	}
	if err != nil {
		f.currentRow = nil
		f.ioErr = err
		return false
	}
	if f.currentRow == nil {
		f.currentRow = &row{}
	}
	f.rowNumber += 1
	f.currentRow.cells = cells
	f.currentRow.missingKeys = nil
	return true
}

func (f *File) RowContent() []string {
	if f.rowNumber == 0 {
		return f.HeaderContent()
	}
	if f.currentRow == nil {
		return []string{}
	}
	return f.currentRow.cells
}

func (f *File) RowNumber() int {
	return f.rowNumber
}

func (f *File) MissingRowKeys() []string {
	return f.currentRow.missingKeys
}

func (f *File) MissingRowKeysWithCondition() []MissingKeyWithCondition {
	return f.currentRow.missingKeysWithCondition
}

func (f *File) Close() error {
	closeErr := f.closer()
	if f.ioErr != nil {
		return f.ioErr
	}
	return closeErr
}

// From: https://stackoverflow.com/a/76023436
//
// BOMAwareCSVReader will detect a UTF BOM (Byte Order Mark) at the
// start of the data and transform to UTF8 accordingly.
// If there is no BOM, it will read the data without any transformation.
func BOMAwareCSVReader(reader io.Reader) *csv.Reader {
	var transformer = unicode.BOMOverride(encoding.Nop.NewDecoder())
	return csv.NewReader(transform.NewReader(reader, transformer))
}
