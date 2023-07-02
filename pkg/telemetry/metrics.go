package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var PromReg = prometheus.NewRegistry()
var Metrics = NewMetrics(PromReg)

type metrics struct {
	CreateSchedulerDurations       prometheus.Histogram
	ServerRequest                  *prometheus.CounterVec
	SchedulerAssignReuse           *prometheus.CounterVec
	SchedulerAssignCreateDurations *prometheus.HistogramVec
	SchedulerIdle                  *prometheus.CounterVec
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
			Help: "How many Assign/Idle requests processed, partitioned by action.",
		}, []string{"method", "action"}),
		SchedulerAssignReuse: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_assign_reuse_total",
			Help: "Scheduler reuse an instance for assign request.",
		}, []string{"app"}),
		// SchedulerRequestDurations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
		// 	Name:    "scheduler_request_duration_milliseconds",
		// 	Help:    "How many Scheduler Assign/Idle requests lantency(ms), partitioned by app(meta key), action and method.",
		// 	Buckets: []float64{},
		// }, []string{"app", "method", "action"}),
		SchedulerAssignCreateDurations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "scheduler_assign_create_duration_milliseconds",
			Help:    "How many Scheduler Assign request create instance lantency(ms), partitioned by app(meta key), status",
			Buckets: []float64{500, 1000, 3000, 5000, 10000},
		}, []string{"app", "status"}),
		SchedulerIdle: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_idle_total",
			Help: "Scheduler idle request.",
		}, []string{"app", "status"}),
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	reg.MustRegister(
		m.CreateSchedulerDurations,
		m.ServerRequest,
		m.SchedulerAssignReuse,
		m.SchedulerAssignCreateDurations,
		m.SchedulerIdle,
	)
	return m
}
