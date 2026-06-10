package cache

import "time"

type TLSOptions struct {
	Enabled           bool
	CertFile          string
	KeyFile           string
	CAFile            string
	ServerName        string
	ClientCertFile    string
	ClientKeyFile     string
	RequireClientCert bool
}

type Options struct {
	NodeName                    string
	ControlBindAddr             string
	ControlBindPort             int
	ControlAdvertiseAddr        string
	GossipBindAddr              string
	GossipBindPort              int
	AdvertiseAddr               string
	AdvertisePort               int
	PeerNodes                   []string
	PeerDNSName                 string
	PeerDNSPort                 int
	PeerRefreshInterval         time.Duration
	SharedKey                   string
	PartitionCount              int
	ReplicationFactor           int
	CacheSizeBytes              int64
	ControlTimeout              time.Duration
	ReplicationRetryInterval    time.Duration
	ReplicationRetryMaxAttempts int
	ReplicationRetryQueueSize   int
	RepairInterval              time.Duration
	RepairMaxKeysPerCycle       int
	ChurnGracePeriod            time.Duration
	TombstoneTTL                time.Duration
	Namespace                   string
	WriteConcern                WriteConcern
	MetricsBindAddr             string
	MetricsBindPort             int
	FailFast                    bool
	SelfCheck                   bool
	SelfCheckTimeout            time.Duration
	RequireSharedKey            bool
	AllowInsecure               bool
	PeerWarnInterval            time.Duration
	MinReadyPeers               int
	TLS                         TLSOptions
}

func (o Options) withDefaults() Options {
	if o.PartitionCount == 0 {
		o.PartitionCount = 271
	}
	if o.ReplicationFactor == 0 {
		o.ReplicationFactor = 3
	}
	if o.CacheSizeBytes == 0 {
		o.CacheSizeBytes = 1 << 30
	}
	if o.ControlTimeout == 0 {
		o.ControlTimeout = 2 * time.Second
	}
	if o.ReplicationRetryInterval == 0 {
		o.ReplicationRetryInterval = 500 * time.Millisecond
	}
	if o.ReplicationRetryMaxAttempts == 0 {
		o.ReplicationRetryMaxAttempts = 3
	}
	if o.ReplicationRetryQueueSize == 0 {
		o.ReplicationRetryQueueSize = 1024
	}
	if o.RepairInterval == 0 {
		o.RepairInterval = 30 * time.Second
	}
	if o.RepairMaxKeysPerCycle == 0 {
		o.RepairMaxKeysPerCycle = 1000
	}
	if o.ChurnGracePeriod == 0 {
		o.ChurnGracePeriod = 30 * time.Second
	}
	if o.TombstoneTTL == 0 {
		o.TombstoneTTL = 5 * time.Minute
	}
	if o.PeerRefreshInterval == 0 {
		o.PeerRefreshInterval = 30 * time.Second
	}
	if o.WriteConcern == 0 {
		o.WriteConcern = WriteConcernOne
	}
	if o.SelfCheckTimeout == 0 {
		o.SelfCheckTimeout = 1 * time.Second
	}
	if o.PeerWarnInterval == 0 {
		o.PeerWarnInterval = 10 * time.Second
	}
	return o
}
