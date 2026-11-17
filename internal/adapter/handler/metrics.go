package handler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metric naming convention (Prometheus): <namespace>_<subsystem>_<name>_<unit>,
// snake_case, and counters end with _total.
// Keep labels low-cardinality: never use user-controlled values such as
// user_id, client_ip or event_type as label values.
var eventsAcceptedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "analytics",
	Subsystem: "ingest",
	Name:      "events_accepted_total",
	Help:      "Total number of events accepted by POST /event and queued into the pipeline.",
})
