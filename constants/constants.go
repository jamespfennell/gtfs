package constants

type StaticFile string

const (
	AgencyFile         StaticFile = "agency.txt"
	FareAttributesFile StaticFile = "fare_attributes.txt"
	FareRulesFile      StaticFile = "fare_rules.txt"
)

type ScheduleEnity string

const (
	Agency   ScheduleEnity = "agency"
	Route    ScheduleEnity = "route"
	Stop     ScheduleEnity = "stop"
	Fare     ScheduleEnity = "fare"
	FareRule ScheduleEnity = "fare_rule"
)

type ForeignId string

const (
	StopZoneId ForeignId = "stop_zone_id"
)
