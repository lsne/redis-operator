// Created by lsne on 2022-11-06 21:47:07

package services

import (
	"context"
	"crypto/tls"
	"redis-operator/utils/redisutils"
	"time"

	redisv1beta1 "redis-operator/api/v1beta1"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type RedisPod struct {
	logger          logr.Logger
	k8sclient       client.Client
	RedisPodRuntime corev1.Pod
	Role            string
	RedisPodIP      string
	RedisPort       int32
	ClusterNodes    *redisutils.ClusterNodes
	// RedisShard      int
	// RedisReplica    int
	RedisClient *redisutils.RedisClient
}

func NewRedisPod(logger logr.Logger, client client.Client, pod corev1.Pod, port int32, username, password string, cert *tls.Config, renameCommands []redisv1beta1.RedisConfigRenameCommand) *RedisPod {
	rp := &RedisPod{
		logger:          logger,
		k8sclient:       client,
		RedisPodRuntime: pod,
		RedisPodIP:      pod.Status.PodIP,
		RedisPort:       port,
	}

	rp.RedisClient = redisutils.NewRedisClient(rp.RedisPodIP, rp.RedisPort, username, password, 10, cert)
	rp.RedisClient.FlushCommands(renameCommands)
	return rp
}

func (rp *RedisPod) Terminating() bool {
	now := time.Now()

	if rp.RedisPodRuntime.DeletionTimestamp == nil {
		return false
	}
	maxTime := rp.RedisPodRuntime.DeletionTimestamp.Add(redisv1beta1.PodDeleteWait) // adding MaxDuration for configuration
	return maxTime.Before(now)
}

func (rp *RedisPod) Delete(force bool) error {
	if force {
		var s client.GracePeriodSeconds = 0
		return rp.k8sclient.Delete(context.TODO(), &rp.RedisPodRuntime, s)
	}
	return rp.k8sclient.Delete(context.TODO(), &rp.RedisPodRuntime)
}

func (rp *RedisPod) GetRole() string {
	return rp.RedisPodRuntime.GetLabels()[redisv1beta1.RedisRoleKey]
}

func (rp *RedisPod) SetRole(role string) error {
	labels := rp.RedisPodRuntime.GetLabels()
	labels[redisv1beta1.RedisRoleKey] = role
	rp.RedisPodRuntime.SetLabels(labels)
	return rp.k8sclient.Update(context.TODO(), &rp.RedisPodRuntime)
}

func (rp *RedisPod) RedisStatus() bool {
	if err := rp.RedisClient.Ping(); err != nil {
		rp.logger.Error(err, "ping 节点失败")
		return false
	}
	return true
}

func (rp *RedisPod) GetInfoReplication() (redisutils.InfoReplication, error) {
	return rp.RedisClient.InfoReplication()
}

func (rp *RedisPod) ReplicaOf(ip string, port string) error {
	rp.logger.Info("在 %s 机器上执行: slaveof %s %s\n", rp.RedisPodIP, ip, port)
	return rp.RedisClient.ReplicaOf(ip, port)
}

func (rp *RedisPod) GetClusterNodes() error {
	var err error
	if rp.ClusterNodes, err = rp.RedisClient.ClusterNodes(); err != nil {
		return err
	}
	return nil
}

func (rp *RedisPod) ClusterForget(id string) error {
	return rp.RedisClient.ClusterForget(id)
}

func (rp *RedisPod) ClusterMeet(ip, port string) error {
	return rp.RedisClient.ClusterMeet(ip, port)
}

func (rp *RedisPod) AttachSlaveToMaster(masterID string) error {
	return rp.RedisClient.AttachSlaveToMaster(masterID)
}

func (rp *RedisPod) AddSlots(slots []redisutils.Slot) error {
	return rp.RedisClient.AddSlots(slots)
}
