// Created by lsne on 2022-11-06 21:51:41

package redisutils

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	redisv1beta1 "redis-operator/api/v1beta1"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

func GetRedisCert(cacert, cert, key []byte, servername string) (*tls.Config, error) {
	var redisCertCA *x509.CertPool
	var redisCerts []tls.Certificate

	redisCertCA = x509.NewCertPool()
	ok := redisCertCA.AppendCertsFromPEM(cacert)
	if !ok {
		return nil, errors.New("root CAs not ok")
	}

	redisCert, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	redisCerts = append(redisCerts, redisCert)

	return &tls.Config{
		RootCAs:      redisCertCA,
		Certificates: redisCerts,
		ServerName:   servername,
		MinVersion:   2,
		ClientAuth:   0,
	}, nil
}

type RedisCommand struct {
	Config       string
	Cluster      string
	Info         string
	BGSave       string
	Debug        string
	Save         string
	Shutdown     string
	SlaveOF      string
	ReplicaOF    string
	BGrewriteaof string
	Migrate      string
}

func NewRedisCommand() *RedisCommand {
	return &RedisCommand{
		Config:       "CONFIG",
		Cluster:      "CLUSTER",
		Info:         "INFO",
		BGSave:       "BGSAVE",
		Debug:        "DEBUG",
		Save:         "SAVE",
		Shutdown:     "SHUTDOWN",
		SlaveOF:      "SLAVEOF",
		ReplicaOF:    "REPLICAOF",
		BGrewriteaof: "BGREWRITEAOF",
		Migrate:      "MIGRATE",
	}
}

type RedisClient struct {
	RedisCommand *RedisCommand
	Host         string
	Port         int32
	Username     string
	Password     string
	Timeout      int
	Cert         *tls.Config
	Conn         *redis.Client
}

func NewRedisClient(host string, port int32, username, password string, timeout int, cert *tls.Config) *RedisClient {
	c := &RedisClient{
		RedisCommand: NewRedisCommand(),
		Host:         host,
		Port:         port,
		Username:     username,
		Password:     password,
		Timeout:      timeout,
		Cert:         cert,
	}

	c.Conn = redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", c.Host, c.Port),
		Username:     c.Username,
		Password:     c.Password,
		DialTimeout:  time.Duration(c.Timeout) * time.Second,
		ReadTimeout:  time.Duration(c.Timeout) * time.Second,
		WriteTimeout: time.Duration(c.Timeout) * time.Second,
		TLSConfig:    cert,
	})

	return c
}

func (c *RedisClient) FlushCommands(renameCommands []redisv1beta1.RedisConfigRenameCommand) {
	if renameCommands == nil {
		return
	}
	for _, renameCommand := range renameCommands {
		if strings.Trim(renameCommand.OriginalCommand, " ") == "" {
			continue
		}
		switch strings.ToUpper(strings.Trim(renameCommand.OriginalCommand, " ")) {
		case c.RedisCommand.Config:
			c.RedisCommand.Config = strings.Trim(renameCommand.NewCommand, " ")
		case c.RedisCommand.Cluster:
			c.RedisCommand.Cluster = strings.Trim(renameCommand.NewCommand, " ")
		case c.RedisCommand.Info:
			c.RedisCommand.Info = strings.Trim(renameCommand.NewCommand, " ")
		case c.RedisCommand.BGSave:
			c.RedisCommand.BGSave = strings.Trim(renameCommand.NewCommand, " ")
		case c.RedisCommand.Debug:
			c.RedisCommand.Debug = strings.Trim(renameCommand.NewCommand, " ")
		case c.RedisCommand.Save:
			c.RedisCommand.Save = strings.Trim(renameCommand.NewCommand, " ")
		case c.RedisCommand.Shutdown:
			c.RedisCommand.Shutdown = strings.Trim(renameCommand.NewCommand, " ")
		case c.RedisCommand.SlaveOF:
			c.RedisCommand.SlaveOF = strings.Trim(renameCommand.NewCommand, " ")
		case c.RedisCommand.ReplicaOF:
			c.RedisCommand.ReplicaOF = strings.Trim(renameCommand.NewCommand, " ")
		case c.RedisCommand.BGrewriteaof:
			c.RedisCommand.BGrewriteaof = strings.Trim(renameCommand.NewCommand, " ")
		}
	}
}

func (c *RedisClient) Close() {
	_ = c.Conn.Close()
}

func (c *RedisClient) Ping() error {
	_, err := c.Conn.Ping(context.Background()).Result()
	return err
}

func (c *RedisClient) ReplicaOf(master string, port string) error {
	if _, err := c.Conn.Do(context.Background(), c.RedisCommand.ReplicaOF, master, port).Result(); err != nil {
		return err
	}
	return nil
}

func (c *RedisClient) ClusterID() (string, error) {
	v, err := c.Conn.Do(context.Background(), c.RedisCommand.Cluster, "MYID").Result()
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

func (c *RedisClient) InfoReplication() (InfoReplication, error) {
	var repl InfoReplication
	repl.Slaves = make(map[string]SlaveInfo)

	slaveRegexp := regexp.MustCompile(`^slave[0-9]{1,5}$`)

	v, err := c.Conn.Do(context.Background(), c.RedisCommand.Info, "Replication").Result()
	if err != nil {
		return repl, err
	}
	s := v.(string)
	for _, line := range strings.Split(s, "\n") {
		line = strings.Trim(line, "\r")
		if strings.Contains(line, ":") {
			kv := strings.Split(line, ":")
			key := strings.Trim(kv[0], " ")
			value := strings.Trim(kv[1], " ")
			switch key {
			case "role":
				repl.Role = value
			case "master_host":
				repl.MasterHost = value
			case "master_port":
				repl.MasterPort, _ = strconv.Atoi(value)
			case "master_link_status":
				repl.MasterLinkStatus = value
			case "connected_slaves":
				repl.ConnectedSlaves, _ = strconv.Atoi(value)
			}

			if slaveRegexp.MatchString(key) {
				slaveinfo, err := parseSlave(value)
				if err != nil {
					return repl, err
				}
				repl.Slaves[slaveinfo.IPPort] = slaveinfo
			}
		}
	}
	return repl, nil
}

func (c *RedisClient) ClusterNodes() (*ClusterNodes, error) {
	nodes := &ClusterNodes{Nodes: make(map[string]*ClusterNode)}

	v, err := c.Conn.Do(context.Background(), c.RedisCommand.Cluster, "NODES").Result()
	if err != nil {
		return nodes, err
	}
	s := v.(string)

	for _, line := range strings.Split(s, "\n") {
		node := ClusterNode{}
		line = strings.Trim(line, "\r")
		if line == "" {
			continue
		}
		nodeInfo := strings.Split(line, " ")
		if len(nodeInfo) < 8 {
			continue
		}

		node.ID = nodeInfo[0]

		IPPort := strings.Split(nodeInfo[1], "@")
		hostPort := strings.Split(IPPort[0], ":")
		port, err := strconv.Atoi(hostPort[1])
		if err != nil {
			return nodes, err
		}

		node.IPPort = IPPort[0]
		node.IP = hostPort[0]
		node.Port = port

		vals := strings.Split(nodeInfo[2], ",")
		for _, val := range vals {
			switch val {
			case RedisMasterRole:
				node.Role = RedisMasterRole
			case RedisSlaveRole:
				node.Role = RedisSlaveRole
			case RedisClusterMyself:
				node.Myself = true
			case NodeStatusFail:
				node.FailStatus = append(node.FailStatus, NodeStatusFail)
			case NodeStatusPFail:
				node.FailStatus = append(node.FailStatus, NodeStatusPFail)
			case NodeStatusHandshake:
				node.FailStatus = append(node.FailStatus, NodeStatusHandshake)
			case NodeStatusNoAddr:
				node.FailStatus = append(node.FailStatus, NodeStatusNoAddr)
			case NodeStatusNoFlags:
				node.FailStatus = append(node.FailStatus, NodeStatusNoFlags)
			}
		}

		if nodeInfo[3] != "-" {
			node.MasterID = nodeInfo[3]
		}

		if i, err := strconv.ParseInt(nodeInfo[4], 10, 64); err == nil {
			node.PingSent = i
		}
		if i, err := strconv.ParseInt(nodeInfo[5], 10, 64); err == nil {
			node.PongRecv = i
		}
		if i, err := strconv.ParseInt(nodeInfo[6], 10, 64); err == nil {
			node.ConfigEpoch = i
		}

		switch nodeInfo[7] {
		case RedisLinkStateConnected:
			node.LinkState = RedisLinkStateConnected
		case RedisLinkStateDisconnected:
			node.LinkState = RedisLinkStateDisconnected
		}

		for _, slot := range nodeInfo[8:] {
			if s, importing, migrating, err := DecodeSlotRange(slot); err == nil {
				node.Slots = append(node.Slots, s...)
				if importing != nil {
					node.ImportingSlots[importing.SlotID] = importing.FromNodeID
				}
				if migrating != nil {
					node.MigratingSlots[migrating.SlotID] = migrating.ToNodeID
				}
			}
		}

		if node.Myself {
			nodes.MyNode = &node
		} else {
			nodes.Nodes[node.IPPort] = &node
			// nodes.Nodes = append(nodes.Nodes, &node)
		}
	}
	return nodes, nil
}

func (c *RedisClient) ClusterForget(id string) error {
	if _, err := c.Conn.Do(context.Background(), c.RedisCommand.Cluster, "forget", id).Result(); err != nil {
		return err
	}
	return nil
}

func (c *RedisClient) ClusterMeet(ip, port string) error {
	if _, err := c.Conn.Do(context.Background(), c.RedisCommand.Cluster, "MEET", ip, port).Result(); err != nil {
		return err
	}
	return nil
}

func (c *RedisClient) AttachSlaveToMaster(masterID string) error {
	if _, err := c.Conn.Do(context.Background(), c.RedisCommand.Cluster, "REPLICATE", masterID).Result(); err != nil {
		return err
	}
	return nil
}

func (c *RedisClient) AddSlots(slots []Slot) error {
	args := make([]interface{}, 2+len(slots))
	args[0] = c.RedisCommand.Cluster
	args[1] = "addslots"
	for i, num := range slots {
		args[2+i] = int(num)
	}
	if _, err := c.Conn.Do(context.Background(), args...).Result(); err != nil {
		return err
	}
	return nil
}

func (c *RedisClient) ClusterSetslotImporting(slot Slot, srcID string) error {
	if _, err := c.Conn.Do(context.Background(), c.RedisCommand.Cluster, "setslot", int(slot), "importing", srcID).Result(); err != nil {
		return err
	}
	return nil
}

func (c *RedisClient) ClusterSetslotMigrating(slot Slot, dstID string) error {
	if _, err := c.Conn.Do(context.Background(), c.RedisCommand.Cluster, "setslot", int(slot), "migrating", dstID).Result(); err != nil {
		return err
	}
	return nil
}

func (c *RedisClient) ClusterGetkeysinslot(slot Slot, batch int) ([]string, error) {
	return c.Conn.Do(context.Background(), c.RedisCommand.Cluster, "getkeysinslot", int(slot), batch).StringSlice()
}

func (c *RedisClient) ClusterMigrate(ip string, port string, keys []string, timeout string) error {
	var args []interface{}
	args = append(args, c.RedisCommand.Migrate, ip, port, "", "0", timeout)
	if c.Password != "" {
		if c.Username == "" {
			args = append(args, "AUTH", c.Password)
		} else {
			args = append(args, "AUTH2", c.Username, c.Password)
		}
	}
	args = append(args, "KEYS")
	for _, key := range keys {
		args = append(args, key)
	}

	if _, err := c.Conn.Do(context.Background(), args...).Result(); err != nil {
		return err
	}
	return nil
}

func (c *RedisClient) ClusterSetslotNode(slot Slot, ID string) error {
	if _, err := c.Conn.Do(context.Background(), c.RedisCommand.Cluster, "setslot", int(slot), "node", ID).Result(); err != nil {
		return err
	}
	return nil
}

func (c *RedisClient) ClusterSetslotStable(slot Slot) error {
	if _, err := c.Conn.Do(context.Background(), c.RedisCommand.Cluster, "setslot", int(slot), "stable").Result(); err != nil {
		return err
	}
	return nil
}
