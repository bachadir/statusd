// Package model holds the status vocabulary shared by the evaluator, the
// replicated store and the API. Everything here is pure (no I/O, no clock), so
// the raft FSM can use it deterministically.
package model

import "fmt"

// Level is the internal signal/component health: 2 OK, 1 degraded, 0 down.
// NoData means "we could not tell" and is never published.
type Level int

const (
	NoData   Level = -1
	Down     Level = 0
	Degraded Level = 1
	OK       Level = 2
)

func (l Level) String() string {
	switch l {
	case OK:
		return "ok"
	case Degraded:
		return "degraded"
	case Down:
		return "down"
	default:
		return "no_data"
	}
}

// Status is the public component status.
type Status string

const (
	Operational         Status = "operational"
	DegradedPerformance Status = "degraded_performance"
	PartialOutage       Status = "partial_outage"
	MajorOutage         Status = "major_outage"
	UnderMaintenance    Status = "under_maintenance"
)

var statusSeverity = map[Status]int{
	Operational:         0,
	UnderMaintenance:    1,
	DegradedPerformance: 2,
	PartialOutage:       3,
	MajorOutage:         4,
}

// AllStatuses in display order (best to worst).
var AllStatuses = []Status{Operational, UnderMaintenance, DegradedPerformance, PartialOutage, MajorOutage}

func (s Status) Valid() bool { _, ok := statusSeverity[s]; return ok }

// Severity orders statuses for roll-ups; higher is worse.
func (s Status) Severity() int { return statusSeverity[s] }

// Label is the human text used on the page.
func (s Status) Label() string {
	switch s {
	case Operational:
		return "Operational"
	case DegradedPerformance:
		return "Degraded performance"
	case PartialOutage:
		return "Partial outage"
	case MajorOutage:
		return "Major outage"
	case UnderMaintenance:
		return "Under maintenance"
	}
	return string(s)
}

// Worst returns the most severe of the given statuses (Operational if none).
func Worst(ss ...Status) Status {
	w := Operational
	for _, s := range ss {
		if s.Severity() > w.Severity() {
			w = s
		}
	}
	return w
}

// ParseStatus validates a status string.
func ParseStatus(s string) (Status, error) {
	st := Status(s)
	if !st.Valid() {
		return "", fmt.Errorf("invalid status %q", s)
	}
	return st, nil
}

// Mapping converts an internal Level to a public Status for one component.
type Mapping struct {
	DegradedAs Status // default DegradedPerformance
	DownAs     Status // default MajorOutage
}

func (m Mapping) StatusFor(l Level) Status {
	switch l {
	case Degraded:
		if m.DegradedAs != "" {
			return m.DegradedAs
		}
		return DegradedPerformance
	case Down:
		if m.DownAs != "" {
			return m.DownAs
		}
		return MajorOutage
	default:
		return Operational
	}
}

// Override is a manual status set from the control panel.
type Override struct {
	Status  Status
	Reason  string
	By      string
	SetAt   int64
	Expires int64 // unix seconds; 0 = until cleared (manual mode)
}

func (o *Override) ActiveAt(ts int64) bool {
	return o != nil && o.Status != "" && (o.Expires == 0 || ts < o.Expires)
}

// Source says why a component has its current effective status.
type Source string

const (
	SourceAuto        Source = "auto"
	SourceManual      Source = "manual"
	SourceMaintenance Source = "maintenance"
	SourceStatic      Source = "static" // no automated signals; defaults to operational
)

// Effective resolves the published status: an override always wins, then an
// active maintenance window, then the automatic (hysteresis-filtered) status.
func Effective(auto Status, automated bool, ov *Override, inMaintenance bool, ts int64) (Status, Source) {
	if ov.ActiveAt(ts) {
		return ov.Status, SourceManual
	}
	if inMaintenance {
		return UnderMaintenance, SourceMaintenance
	}
	if auto == "" {
		auto = Operational
	}
	if !automated {
		return auto, SourceStatic
	}
	return auto, SourceAuto
}

// Incident lifecycle states.
const (
	IncidentInvestigating = "investigating"
	IncidentIdentified    = "identified"
	IncidentMonitoring    = "monitoring"
	IncidentResolved      = "resolved"
)

var IncidentStates = []string{IncidentInvestigating, IncidentIdentified, IncidentMonitoring, IncidentResolved}

func ValidIncidentState(s string) bool {
	for _, x := range IncidentStates {
		if x == s {
			return true
		}
	}
	return false
}

// Incident impact levels (page-level severity of an incident).
var IncidentImpacts = []string{"none", "minor", "major", "critical"}

func ValidImpact(s string) bool {
	for _, x := range IncidentImpacts {
		if x == s {
			return true
		}
	}
	return false
}

// Announcement levels.
var AnnouncementLevels = []string{"info", "warning", "critical"}

func ValidAnnouncementLevel(s string) bool {
	for _, x := range AnnouncementLevels {
		if x == s {
			return true
		}
	}
	return false
}
