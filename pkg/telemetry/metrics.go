package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var PromReg = prometheus.NewRegistry()
var Metrics = NewMetrics(PromReg)

type metrics struct {
	CreateSchedulerDurations prometheus.Histogram
	ServerRequest            *prometheus.CounterVec
	SchedulerRequest         *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		CreateSchedulerDurations: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "server_create_scheduler_duration_microseconds",
			Help:    "A histogram of the scaler create scheduler durations in microseconds(µs).",
			Buckets: []float64{100, 200, 300, 500, 1000, 3000, 5000},
		}),
		ServerRequest: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "server_request_total",
			Help: "How many Assign/Idle requests processed, partitioned by status.",
		}, []string{"method", "status"}),
		SchedulerRequest: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_request_total",
			Help: "How many Scheduler Assign/Idle requests processed, partitioned by app(meta key), status and method.",
		}, []string{"app", "method", "status"}),
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	reg.MustRegister(
		m.CreateSchedulerDurations,
		m.ServerRequest,
		m.SchedulerRequest,
	)
	return m
}
