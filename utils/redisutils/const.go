// Created by lsne on 2022-11-06 21:50:44

package redisutils

const (
	// DefaultRedisPort define the default Redis Port
	DefaultRedisPort   = "6379"
	RedisClusterMyself = "myself"
	// RedisMasterRole redis role master
	RedisMasterRole = "master"
	// RedisSlaveRole redis role slave
	RedisSlaveRole = "slave"
)

const (
	// RedisLinkStateConnected redis connection status connected
	RedisLinkStateConnected = "connected"
	// RedisLinkStateDisconnected redis connection status disconnected
	RedisLinkStateDisconnected = "disconnected"
)

const (
	// NodeStatusPFail Node is in PFAIL state. Not reachable for the node you are contacting, but still logically reachable
	NodeStatusPFail = "fail?"
	// NodeStatusFail Node is in FAIL state. It was not reachable for multiple nodes that promoted the PFAIL state to FAIL
	NodeStatusFail = "fail"
	// NodeStatusHandshake Untrusted node, we are handshaking.
	NodeStatusHandshake = "handshake"
	// NodeStatusNoAddr No address known for this node
	NodeStatusNoAddr = "noaddr"
	// NodeStatusNoFlags no flags at all
	NodeStatusNoFlags = "noflags"
)
