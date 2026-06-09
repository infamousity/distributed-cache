package metrics

import (
	"fmt"
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry *prometheus.Registry
	server   *http.Server
	ln       net.Listener

	GetTotal              prometheus.Counter
	HitTotal              prometheus.Counter
	MissTotal             prometheus.Counter
	ForwardTotal          prometheus.Counter
	ForwardErrorTotal     prometheus.Counter
	ReplicationSuccess    prometheus.Counter
	ReplicationError      prometheus.Counter
	ReplicationRetry      prometheus.Counter
	ReplicationRetryDrop  prometheus.Counter
	ReadRepairTotal       prometheus.Counter
	AntiEntropyTotal      prometheus.Counter
	WriteQuorumFailed     prometheus.Counter
	PeerAuthFail          prometheus.Counter
	PeerTLSFail           prometheus.Counter
	PeerUnreachable       prometheus.Counter
	ChurnCleanupDelayed   prometheus.Counter
	ChurnCleanupApplied   prometheus.Counter
	RingSize              prometheus.Gauge
	VerifiedPeers         prometheus.Gauge
	UnreachablePeers      prometheus.Gauge
	IdentityMismatchPeers prometheus.Gauge
	Ready                 prometheus.Gauge
	RetryQueueDepth       prometheus.Gauge
	GossipMessages        prometheus.Gauge
	GossipDegradedEvents  prometheus.Gauge
}

func New(bindAddr string, bindPort int) (*Metrics, error) {
	if bindAddr == "" || bindPort == 0 {
		return nil, nil
	}

	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		GetTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_get_total",
			Help: "Total cache get requests",
		}),
		HitTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_hit_total",
			Help: "Total cache hits",
		}),
		MissTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_miss_total",
			Help: "Total cache misses",
		}),
		ForwardTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_forward_total",
			Help: "Total forwarded requests",
		}),
		ForwardErrorTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_forward_error_total",
			Help: "Total forwarding errors",
		}),
		ReplicationSuccess: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_replication_success_total",
			Help: "Total successful replications",
		}),
		ReplicationError: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_replication_error_total",
			Help: "Total replication errors",
		}),
		ReplicationRetry: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_replication_retry_total",
			Help: "Total replication retries enqueued",
		}),
		ReplicationRetryDrop: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_replication_retry_drop_total",
			Help: "Total replication retries dropped due to full queue",
		}),
		ReadRepairTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_read_repair_total",
			Help: "Total read repair attempts",
		}),
		AntiEntropyTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_anti_entropy_total",
			Help: "Total anti-entropy repair actions",
		}),
		WriteQuorumFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_write_quorum_failed_total",
			Help: "Total writes that failed to reach quorum",
		}),
		PeerAuthFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_peer_auth_fail_total",
			Help: "Total peer auth failures",
		}),
		PeerTLSFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_peer_tls_fail_total",
			Help: "Total peer TLS failures",
		}),
		PeerUnreachable: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_peer_unreachable_total",
			Help: "Total peer unreachable failures",
		}),
		ChurnCleanupDelayed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_churn_cleanup_delayed_total",
			Help: "Total ownership-loss cleanup actions delayed by churn grace",
		}),
		ChurnCleanupApplied: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_churn_cleanup_applied_total",
			Help: "Total ownership-loss cleanup actions applied after churn grace",
		}),
		RingSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cache_ring_size",
			Help: "Current number of nodes in the local ring",
		}),
		VerifiedPeers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cache_verified_peers",
			Help: "Current number of verified peers excluding self",
		}),
		UnreachablePeers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cache_unreachable_peers",
			Help: "Current number of peers marked unreachable",
		}),
		IdentityMismatchPeers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cache_identity_mismatch_peers",
			Help: "Current number of peers with control-plane identity mismatches",
		}),
		Ready: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cache_ready",
			Help: "Whether the cache node currently satisfies readiness checks",
		}),
		RetryQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cache_retry_queue_depth",
			Help: "Current number of pending replication retry tasks",
		}),
		GossipMessages: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cache_gossip_log_messages",
			Help: "Total memberlist log messages observed by this node",
		}),
		GossipDegradedEvents: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cache_gossip_degraded_events",
			Help: "Total memberlist log messages that indicate degraded gossip transport",
		}),
	}

	reg.MustRegister(
		m.GetTotal,
		m.HitTotal,
		m.MissTotal,
		m.ForwardTotal,
		m.ForwardErrorTotal,
		m.ReplicationSuccess,
		m.ReplicationError,
		m.ReplicationRetry,
		m.ReplicationRetryDrop,
		m.ReadRepairTotal,
		m.AntiEntropyTotal,
		m.WriteQuorumFailed,
		m.PeerAuthFail,
		m.PeerTLSFail,
		m.PeerUnreachable,
		m.ChurnCleanupDelayed,
		m.ChurnCleanupApplied,
		m.RingSize,
		m.VerifiedPeers,
		m.UnreachablePeers,
		m.IdentityMismatchPeers,
		m.Ready,
		m.RetryQueueDepth,
		m.GossipMessages,
		m.GossipDegradedEvents,
	)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	m.server = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", bindAddr, bindPort),
		Handler: mux,
	}
	return m, nil
}

func (m *Metrics) Start() error {
	if m == nil || m.server == nil {
		return nil
	}
	ln, err := net.Listen("tcp", m.server.Addr)
	if err != nil {
		return err
	}
	m.ln = ln
	go func() {
		_ = m.server.Serve(ln)
	}()
	return nil
}

func (m *Metrics) Stop() error {
	if m == nil || m.server == nil {
		return nil
	}
	return m.server.Close()
}
