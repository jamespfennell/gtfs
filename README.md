# gtfs

This is a Go library for parsing GTFS static and realtime feeds.

The static parser is pretty straightforward and simply maps fields in the 
    GTFS static CSV files to associated Go types.
The realtime parser, on the other hand, is fairly opinionated and does
    some data restructuring in order to make the results (we think)
    easier to work with.
Details about this are outlined below.
The realtime parser is also designed to handle GTFS extensions, and currently
   supports the NYC Subway extension.

## Examples

Parse the GTFS static feed for the New York City Subway:

```go

```

Parse the GTFS realtime feed for the San Francisco Bay Area BART:

```go

```

Parse the GTFS realtime feed for the New York City Subway's G train:

```go

```

## Command line tool

## Static parser

## Realtime parser

What the realtime parser adds:

- Restuctures the proto message to
  - have the concept of Trip and Vehicle
  - make more explicit the link between these
  - create implicit trips and vehicles
- Converts to native Go types like Time and Duration
- Handles extensions (currently just the NYCT)
