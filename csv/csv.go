// Package csv is a wrapper around the stdlib csv library that provides a nice API for the GTFS static parser.
//
// Because, of course, everything can be solved with another layer of indirection.
package csv

import (
	"encoding/csv"
	"fmt"
	"io"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

type File struct {
	csvReader              *csv.Reader
	headerMap              map[string]int
	missingRequiredColumns []string
	currentRow             *row
	ioErr                  error
	closer                 func() error
}

type row struct {
	cells       []string
	missingKeys []string
}

func New(reader io.ReadCloser) (*File, error) {
	csvReader := BOMAwareCSVReader(reader)
	csvReader.ReuseRecord = true
	firstRow, err := csvReader.Read()
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
		headerMap: m,
		csvReader: csvReader,
		closer:    reader.Close,
	}, nil
}

type RequiredColumn struct {
	i int
	s string
	f *File
}

func (f *File) RequiredColumn(s string) RequiredColumn {
	i, b := f.headerMap[s]
	if !b {
		f.missingRequiredColumns = append(f.missingRequiredColumns, s)
		i = -1
	}
	return RequiredColumn{i, s, f}
}

func (p *File) MissingRequiredColumns() error {
	if len(p.missingRequiredColumns) == 0 {
		return nil
	}
	return fmt.Errorf("missing required columns %s", p.missingRequiredColumns)
}

func (c RequiredColumn) Read() string {
	r := c.f.currentRow
	if c.i >= len(r.cells) || r.cells[c.i] == "" {
		r.missingKeys = append(r.missingKeys, c.s)
		return ""
	}
	return c.f.currentRow.cells[c.i]
}

type OptionalColumn struct {
	i int
	f *File
}

func (f *File) OptionalColumn(s string) OptionalColumn {
	i, b := f.headerMap[s]
	if !b {
		i = -1
	}
	return OptionalColumn{i: i, f: f}
}

func (c OptionalColumn) Read() *string {
	if c.i < 0 {
		return nil
	}
	// We copy the string pointer because the CSV library reuses the byte array across rows.
	s := c.f.currentRow.cells[c.i]
	return &s
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
	f.currentRow.cells = cells
	f.currentRow.missingKeys = nil
	return true
}

func (f *File) MissingRowKeys() []string {
	return f.currentRow.missingKeys
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
