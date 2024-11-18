// Created by lsne on 2022-11-06 21:51:16

package redisutils

type RedisClusterInfo struct {
	ClusterState         string
	ClusterSlotsAssigned uint64
	ClusterSlotsOk       uint64
	ClusterSlotsPfail    uint64
	ClusterSlotsFail     uint64
	ClusterKnownNodes    uint64
	ClusterSize          uint64
	ClusterCurrentEpoch  uint64
	ClusterMyEpoch       uint64
}

type DatabaseInfo struct {
	DBName  string //db0
	Keys    uint64
	Expires uint64
	AvgTtl  uint64
}

type SlaveInfo struct {
	IPPort string
	IP     string
	Port   int
	State  string
	Offset uint64
	Lag    uint64
}

type InfoReplication struct {
	Role             string
	MasterHost       string
	MasterPort       int
	MasterLinkStatus string
	ConnectedSlaves  int
	Slaves           map[string]SlaveInfo
}
