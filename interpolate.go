package gtfs

import (
	"time"
)

// InterpolateStopTimes Helper: interpolate missing arrival or departure times evenly
func interpolateStopTimes(times []ScheduledStopTime) []ScheduledStopTime {
	// Work on a copy so the original slice is not modified outside
	result := make([]ScheduledStopTime, len(times))
	copy(result, times)
	n := len(result)
	if n == 0 {
		return nil
	}

	// Interpolate Arrival and Departure separately
	for tType := 0; tType < 2; tType++ {
		// tType: 0 = ArrivalTime, 1 = DepartureTime
		i := 0
		for i < n {
			// Find the next known time
			if getTime(&result[i], tType) == 0 {
				// Start of missing segment
				startIdx := i - 1
				startTime := time.Duration(0)
				if startIdx >= 0 {
					startTime = getTime(&result[startIdx], tType)
				}
				// Find end of missing segment
				endIdx := i
				for endIdx < n && getTime(&result[endIdx], tType) == 0 {
					endIdx++
				}
				endTime := time.Duration(0)
				if endIdx < n {
					endTime = getTime(&result[endIdx], tType)
				}
				intervals := endIdx - startIdx
				if startIdx >= 0 && endIdx < n && endTime > startTime && intervals > 0 {
					delta := (endTime - startTime) / time.Duration(intervals)
					for j := 1; j < intervals; j++ {
						setTime(&result[startIdx+j], tType, startTime+time.Duration(j)*delta)
					}
				}
				i = endIdx
			} else {
				i++
			}
		}
	}
	return result
}

func interpolateStopTimesByShapeDist(times []ScheduledStopTime) []ScheduledStopTime {
	result := make([]ScheduledStopTime, len(times))
	copy(result, times)
	n := len(result)
	if n == 0 {
		return nil
	}

	for tType := 0; tType < 2; tType++ {
		i := 0
		for i < n {
			if getTime(&result[i], tType) == 0 {
				startIdx := i - 1
				var startTime time.Duration
				var startDist float64
				if startIdx >= 0 && result[startIdx].ShapeDistanceTraveled != nil {
					startTime = getTime(&result[startIdx], tType)
					startDist = *result[startIdx].ShapeDistanceTraveled
				}
				endIdx := i
				for endIdx < n && getTime(&result[endIdx], tType) == 0 {
					endIdx++
				}
				var endTime time.Duration
				var endDist float64
				if endIdx < n && result[endIdx].ShapeDistanceTraveled != nil {
					endTime = getTime(&result[endIdx], tType)
					endDist = *result[endIdx].ShapeDistanceTraveled
				}
				intervals := endIdx - startIdx
				if startIdx >= 0 && endIdx < n && endDist > startDist && endTime > startTime && intervals > 0 {
					for j := 1; j < intervals; j++ {
						dist := *result[startIdx+j].ShapeDistanceTraveled
						w := (dist - startDist) / (endDist - startDist)
						interpolated := startTime + time.Duration(float64(endTime-startTime)*w)
						setTime(&result[startIdx+j], tType, interpolated)
					}
				}
				i = endIdx
			} else {
				i++
			}
		}
	}
	return result
}

// Helpers to get/set arrival/departure by index
func getTime(stop *ScheduledStopTime, tType int) time.Duration {
	if tType == 0 {
		return stop.ArrivalTime
	}
	return stop.DepartureTime
}
func setTime(stop *ScheduledStopTime, tType int, t time.Duration) {
	if tType == 0 {
		stop.ArrivalTime = t
	} else {
		stop.DepartureTime = t
	}
}
