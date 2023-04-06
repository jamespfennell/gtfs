# Performance

## Profiler

This directory contains a performance profiler for the package, in `profiler.go`.
It currently only works with GTFS static files.

The following is reasonable chain of commands to run a profile and then view the results in the browser.
It assumes that the [pprof CLI tool](https://github.com/google/pprof) is installed (`go install github.com/google/pprof@latest`).

```
go test ./... && \
   go build performance/profiler.go && \
   ./profiler tmp/*.zip && \
   pprof --http=0.0.0.0:1234 ./pprof ./gtfs_package_profile.pb.gz
```

Explanation:

1. Makes sure the tests are passing (no point in profiling otherwise!).

1. Builds the tool.

1. Runs the tool over all zip files in the `./tmp` directory.

1. Launches a web viewer for the results.
