// Created by lsne on 2022-11-06 21:40:37

package v1beta1

import "fmt"

type RedisConfigClientOutputBufferLimitRow struct {
	MaxValue string `json:"maxvalue"`
	Value    string `json:"value"`
	Duration int32  `json:"duration"`
}

func (row *RedisConfigClientOutputBufferLimitRow) Validate() error {
	return nil
}

type RedisConfigClientOutputBufferLimit struct {
	Normal RedisConfigClientOutputBufferLimitRow `json:"normal"`
	Pubsub RedisConfigClientOutputBufferLimitRow `json:"pubsub"`
	Slave  RedisConfigClientOutputBufferLimitRow `json:"slave"`
}

func (limit *RedisConfigClientOutputBufferLimit) ToString() string {
	normal := fmt.Sprintf("client-output-buffer-limit normal %s %s %d", limit.Normal.MaxValue, limit.Normal.Value, limit.Normal.Duration)
	pubsub := fmt.Sprintf("client-output-buffer-limit normal %s %s %d", limit.Pubsub.MaxValue, limit.Pubsub.Value, limit.Pubsub.Duration)
	slave := fmt.Sprintf("client-output-buffer-limit normal %s %s %d", limit.Slave.MaxValue, limit.Slave.Value, limit.Slave.Duration)
	return fmt.Sprintf("%s\n%s\n%s", normal, pubsub, slave)
}

type RedisConfigSave struct {
	Seconds int64 `json:"seconds"`
	Changes int64 `json:"changes"`
}

type RedisConfigRenameCommand struct {
	OriginalCommand string `json:"original-command"`
	NewCommand      string `json:"new-command"`
}

type RedisConfig struct {
	Activerehashing            string `json:"activerehashing"`
	Activedefrag               string `json:"activedefrag"`
	ActiveDefragCycleMin       uint64 `json:"active-defrag-cycle-min"`
	ActiveDefragCycleMax       uint64 `json:"active-defrag-cycle-max"`
	ActiveDefragIgnoreBytes    string `json:"active-defrag-ignore-bytes"`
	ActiveDefragThresholdLower uint64 `json:"active-defrag-threshold-lower"`
	ActiveDefragThresholdUpper uint64 `json:"active-defrag-threshold-upper"`

	Appendonly               string `json:"appendonly"`
	Appendfsync              string `json:"appendfsync"`
	AofLoadTruncated         string `json:"aof-load-truncated"`
	AofUseRdbPreamble        string `json:"aof-use-rdb-preamble"`
	NoAppendfsyncOnRewrite   string `json:"no-appendfsync-on-rewrite"`
	AutoAofRewritePercentage uint64 `json:"auto-aof-rewrite-percentage"`
	AutoAofRewriteMinSize    string `json:"auto-aof-rewrite-min-size"`

	ProtectedMode string `json:"protected-mode"`
	TcpBacklog    uint64 `json:"tcp-backlog"`
	TcpKeepalive  uint64 `json:"tcp-keepalive"`
	Loglevel      string `json:"loglevel"`
	Port          int32  `json:"port"`
	Databases     int32  `json:"databases"`
	// Dir                     string `json:"dir"`        // 无用, 定死为 /redis/data
	// Dbfilename              string `json:"dbfilename"` // 无用, 定死为 dump.rdb
	Rdbchecksum             string `json:"rdbchecksum"`
	Rdbcompression          string `json:"rdbcompression"`
	StopWritesOnBgsaveError string `json:"stop-writes-on-bgsave-error"`
	Timeout                 uint64 `json:"timeout"`
	Maxclients              uint64 `json:"maxclients"`
	Maxmemory               string `json:"maxmemory"`
	MaxmemoryPolicy         string `json:"maxmemory-policy"`
	MaxmemorySamples        uint64 `json:"maxmemory-samples"`
	Hz                      uint64 `json:"hz"`
	ReplBacklogSize         string `json:"repl-backlog-size"`
	ReplBacklogTtl          uint64 `json:"repl-backlog-ttl"`
	SlowlogLogSlowerThan    uint64 `json:"slowlog-log-slower-than"`
	SlowlogMaxLen           uint64 `json:"slowlog-max-len"`

	TlsPort int32 `json:"tls-port"`
	// TlsAuthClients  string `json:"tls-auth-clients"`
	// TlsReplication  string `json:"tls-replication"`
	// TlsCluster      string `json:"tls-cluster"`
	// TlsCertFile     string `json:"tls-cert-file"`
	// TlsKeyFile      string `json:"tls-key-file"`
	// TlsCaCertFile   string `json:"tls-ca-cert-file"`
	// TlsDhParamsFile string `json:"tls-dh-params-file"`

	ClusterEnabled string `json:"cluster-enabled"`
	// ClusterConfigFile          string `json:"cluster-config-file"`
	ClusterMigrationBarrier    uint64 `json:"cluster-migration-barrier"`
	ClusterRequireFullCoverage string `json:"cluster-require-full-coverage"`
	ClusterNodeTimeout         uint64 `json:"cluster-node-timeout"`

	// Masteruser            string `json:"masteruser"`
	// Masterauth            string `json:"masterauth"`
	SetMaxIntsetEntries   uint64 `json:"set-max-intset-entries"`
	HashMaxZiplistEntries uint64 `json:"hash-max-ziplist-entries"`
	HashMaxZiplistValue   uint64 `json:"hash-max-ziplist-value"`
	ListMaxZiplistEntries uint64 `json:"list-max-ziplist-entries"`
	ListMaxZiplistValue   uint64 `json:"list-max-ziplist-value"`
	ZsetMaxZiplistEntries uint64 `json:"zset-max-ziplist-entries"`
	ZsetMaxZiplistValue   uint64 `json:"zset-max-ziplist-value"`

	ClientOutputBufferLimit RedisConfigClientOutputBufferLimit `json:"client-output-buffer-limit"`
	Save                    []RedisConfigSave                  `json:"save"`
	RenameCommand           []RedisConfigRenameCommand         `json:"rename-command"`
}

func NewRedisConfig(tls bool) *RedisConfig {
	c := &RedisConfig{
		Activerehashing:            "yes",
		Activedefrag:               "yes",
		ActiveDefragCycleMin:       20,
		ActiveDefragCycleMax:       50,
		ActiveDefragIgnoreBytes:    "500mb",
		ActiveDefragThresholdLower: 30,
		ActiveDefragThresholdUpper: 100,

		Appendonly:               "no",
		Appendfsync:              "everysec",
		AofLoadTruncated:         "yes",
		AofUseRdbPreamble:        "yes",
		NoAppendfsyncOnRewrite:   "no",
		AutoAofRewritePercentage: 100,
		AutoAofRewriteMinSize:    "256mb",

		ProtectedMode:           "yes",
		TcpBacklog:              511,
		TcpKeepalive:            300,
		Loglevel:                "notice",
		Databases:               16,
		Rdbchecksum:             "yes",
		Rdbcompression:          "yes",
		StopWritesOnBgsaveError: "no",
		Timeout:                 0,
		Maxclients:              20000,
		MaxmemoryPolicy:         "noeviction",
		MaxmemorySamples:        5,
		Hz:                      80,
		ReplBacklogSize:         "512m",
		ReplBacklogTtl:          0,
		SlowlogLogSlowerThan:    10000,
		SlowlogMaxLen:           2048,

		// TlsAuthClients:  "yes",
		// TlsReplication:  "yes",
		// TlsCluster:      "yes",
		// TlsCertFile:     "/redis/cert/redis.crt",
		// TlsKeyFile:      "/redis/cert/redis.key",
		// TlsCaCertFile:   "/redis/cert/ca.crt",
		// TlsDhParamsFile: "/redis/cert/redis.dh",

		ClusterEnabled: "no",
		// ClusterConfigFile:          "nodes.conf",
		ClusterMigrationBarrier:    1,
		ClusterRequireFullCoverage: "yes",
		ClusterNodeTimeout:         60000,

		SetMaxIntsetEntries:   512,
		HashMaxZiplistEntries: 512,
		HashMaxZiplistValue:   64,
		ListMaxZiplistEntries: 512,
		ListMaxZiplistValue:   64,
		ZsetMaxZiplistEntries: 128,
		ZsetMaxZiplistValue:   64,
		ClientOutputBufferLimit: RedisConfigClientOutputBufferLimit{
			Normal: RedisConfigClientOutputBufferLimitRow{
				MaxValue: "0",
				Value:    "0",
				Duration: 0,
			},
			Pubsub: RedisConfigClientOutputBufferLimitRow{
				MaxValue: "0",
				Value:    "0",
				Duration: 0,
			},
			Slave: RedisConfigClientOutputBufferLimitRow{
				MaxValue: "0",
				Value:    "0",
				Duration: 0,
			},
		},
	}

	if tls {
		c.Port = 0
		c.TlsPort = 6379
	} else {
		c.Port = 6379
		c.TlsPort = 0
	}
	return c
}

// save 900 1
// save 300 10
// save 60 10000
func (rc *RedisConfig) SaveToString() string {
	slaves := ""
	for _, save := range rc.Save {
		if slaves == "" {
			slaves = fmt.Sprintf("save %d %d", save.Seconds, save.Changes)
		} else {
			slaves = slaves + fmt.Sprintf("\nsave %d %d", save.Seconds, save.Changes)
		}
	}
	return slaves
}

func (rc *RedisConfig) RenameCommandToString() string {
	cmds := ""
	for _, cmd := range rc.RenameCommand {
		if cmds == "" {
			cmds = fmt.Sprintf("rename-command %s %s", cmd.OriginalCommand, cmd.NewCommand)
		} else {
			cmds = cmds + fmt.Sprintf("\nrename-command %s %s", cmd.OriginalCommand, cmd.NewCommand)
		}
	}
	return cmds
}

func (rc *RedisConfig) InitArgs(tls bool) {
	defaultrc := NewRedisConfig(tls)

	if tls {
		if rc.TlsPort == 0 {
			rc.TlsPort = defaultrc.TlsPort
		}
		rc.Port = 0
	} else {
		if rc.Port == 0 {
			rc.Port = defaultrc.Port
		}
		rc.TlsPort = 0
	}

	if rc.Activerehashing == "" {
		rc.Activerehashing = defaultrc.Activerehashing
	}
	if rc.Activedefrag == "" {
		rc.Activedefrag = defaultrc.Activedefrag
	}
	if rc.ActiveDefragCycleMin == 0 {
		rc.ActiveDefragCycleMin = defaultrc.ActiveDefragCycleMin
	}
	if rc.ActiveDefragCycleMax == 0 {
		rc.ActiveDefragCycleMax = defaultrc.ActiveDefragCycleMax
	}
	if rc.ActiveDefragIgnoreBytes == "" {
		rc.ActiveDefragIgnoreBytes = defaultrc.ActiveDefragIgnoreBytes
	}
	if rc.ActiveDefragThresholdLower == 0 {
		rc.ActiveDefragThresholdLower = defaultrc.ActiveDefragThresholdLower
	}
	if rc.ActiveDefragThresholdUpper == 0 {
		rc.ActiveDefragThresholdUpper = defaultrc.ActiveDefragThresholdUpper
	}

	if rc.Appendonly == "" {
		rc.Appendonly = defaultrc.Appendonly
	}
	if rc.Appendfsync == "" {
		rc.Appendfsync = defaultrc.Appendfsync
	}
	if rc.AofLoadTruncated == "" {
		rc.AofLoadTruncated = defaultrc.AofLoadTruncated
	}
	if rc.AofUseRdbPreamble == "" {
		rc.AofUseRdbPreamble = defaultrc.AofUseRdbPreamble
	}
	if rc.NoAppendfsyncOnRewrite == "" {
		rc.NoAppendfsyncOnRewrite = defaultrc.NoAppendfsyncOnRewrite
	}
	if rc.AutoAofRewritePercentage == 0 {
		rc.AutoAofRewritePercentage = defaultrc.AutoAofRewritePercentage
	}
	if rc.AutoAofRewriteMinSize == "" {
		rc.AutoAofRewriteMinSize = defaultrc.AutoAofRewriteMinSize
	}

	if rc.ProtectedMode == "" {
		rc.ProtectedMode = defaultrc.ProtectedMode
	}
	if rc.TcpBacklog == 0 {
		rc.TcpBacklog = defaultrc.TcpBacklog
	}
	if rc.TcpKeepalive == 0 {
		rc.TcpKeepalive = defaultrc.TcpKeepalive
	}
	if rc.Loglevel == "" {
		rc.Loglevel = defaultrc.Loglevel
	}

	if rc.Databases == 0 {
		rc.Databases = defaultrc.Databases
	}

	if rc.Rdbchecksum == "" {
		rc.Rdbchecksum = defaultrc.Rdbchecksum
	}
	if rc.Rdbcompression == "" {
		rc.Rdbcompression = defaultrc.Rdbcompression
	}
	if rc.StopWritesOnBgsaveError == "" {
		rc.StopWritesOnBgsaveError = defaultrc.StopWritesOnBgsaveError
	}
	if rc.Timeout == 0 {
		rc.Timeout = defaultrc.Timeout
	}
	if rc.Maxclients == 0 {
		rc.Maxclients = defaultrc.Maxclients
	}
	if rc.Maxmemory == "" {
		rc.Maxmemory = defaultrc.Maxmemory
	}
	if rc.MaxmemoryPolicy == "" {
		rc.MaxmemoryPolicy = defaultrc.MaxmemoryPolicy
	}
	if rc.MaxmemorySamples == 0 {
		rc.MaxmemorySamples = defaultrc.MaxmemorySamples
	}
	if rc.Hz == 0 {
		rc.Hz = defaultrc.Hz
	}
	if rc.ReplBacklogSize == "" {
		rc.ReplBacklogSize = defaultrc.ReplBacklogSize
	}
	if rc.ReplBacklogTtl == 0 {
		rc.ReplBacklogTtl = defaultrc.ReplBacklogTtl
	}
	if rc.SlowlogLogSlowerThan == 0 {
		rc.SlowlogLogSlowerThan = defaultrc.SlowlogLogSlowerThan
	}
	if rc.SlowlogMaxLen == 0 {
		rc.SlowlogMaxLen = defaultrc.SlowlogMaxLen
	}

	if rc.ClusterMigrationBarrier == 0 {
		rc.ClusterMigrationBarrier = defaultrc.ClusterMigrationBarrier
	}
	if rc.ClusterRequireFullCoverage == "" {
		rc.ClusterRequireFullCoverage = defaultrc.ClusterRequireFullCoverage
	}
	if rc.ClusterNodeTimeout == 0 {
		rc.ClusterNodeTimeout = defaultrc.ClusterNodeTimeout
	}

	if rc.SetMaxIntsetEntries == 0 {
		rc.SetMaxIntsetEntries = defaultrc.SetMaxIntsetEntries
	}
	if rc.HashMaxZiplistEntries == 0 {
		rc.HashMaxZiplistEntries = defaultrc.HashMaxZiplistEntries
	}
	if rc.HashMaxZiplistValue == 0 {
		rc.HashMaxZiplistValue = defaultrc.HashMaxZiplistValue
	}
	if rc.ListMaxZiplistEntries == 0 {
		rc.ListMaxZiplistEntries = defaultrc.ListMaxZiplistEntries
	}
	if rc.ListMaxZiplistValue == 0 {
		rc.ListMaxZiplistValue = defaultrc.ListMaxZiplistValue
	}
	if rc.ZsetMaxZiplistEntries == 0 {
		rc.ZsetMaxZiplistEntries = defaultrc.ZsetMaxZiplistEntries
	}
	if rc.ZsetMaxZiplistValue == 0 {
		rc.ZsetMaxZiplistValue = defaultrc.ZsetMaxZiplistValue
	}

	if rc.Save == nil {
		rc.Save = defaultrc.Save
	}
	if rc.RenameCommand == nil {
		rc.RenameCommand = defaultrc.RenameCommand
	}
}

func (rc *RedisConfig) Validator() error {
	return nil
}
