// Created by lsne on 2022-11-12 14:40:30

package services

import (
	"context"
	"fmt"
	"math"
	"redis-operator/api/v1beta1"
	redisv1beta1 "redis-operator/api/v1beta1"
	"redis-operator/utils/k8sutils"
	"redis-operator/utils/redisutils"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type RedisClusterService struct {
	logger         logr.Logger
	k8sclient      client.Client
	instance       *redisv1beta1.RedisCluster
	ownerRef       []metav1.OwnerReference
	labels         map[string]string
	RedisConfigMap *corev1.ConfigMap
	Shards         map[string]*RedisStatefulset
	RedisService   *corev1.Service
	RedisPods      []*RedisPod
}

func NewRedisClusterService(logger logr.Logger, client client.Client, instance *redisv1beta1.RedisCluster) *RedisClusterService {
	r := &RedisClusterService{
		logger:    logger,
		k8sclient: client,
		instance:  instance,
		ownerRef:  redisv1beta1.GenerateRedisClusterOwnerReferences(instance),
		labels: map[string]string{
			redisv1beta1.ControllerNameKey: redisv1beta1.ControllerNameValue,
			redisv1beta1.ClusterKindKey:    redisv1beta1.RedisClusterKind,
			redisv1beta1.ClusterNameKey:    redisv1beta1.RedisClusterPrefix + instance.Name,
		},
		Shards: make(map[string]*RedisStatefulset),
	}

	r.RedisConfigMap = &corev1.ConfigMap{
		ObjectMeta: k8sutils.GenerateSTSObjectMeta(r.labels[redisv1beta1.ClusterNameKey]+redisv1beta1.RedisConfigMapNameSuffix, r.instance.Namespace, r.labels, nil, r.ownerRef),
		Data: map[string]string{
			redisv1beta1.RedisStartupScript:     redisv1beta1.RedisStartupScriptContent,
			redisv1beta1.RedisShutdownScript:    redisv1beta1.RedisShutdownScriptContent,
			redisv1beta1.RedisHealthCheckScript: redisv1beta1.RedisHealthCheckScriptContent,
		},
	}
	r.instance.Spec.InitArgs()
	return r
}

func (s *RedisClusterService) Validator() error {
	return nil
}

func (s *RedisClusterService) InitRedisStatefulset() error {
	var stsReplicas int32 = 0

	if s.instance.Spec.Replicas < 0 {
		stsReplicas = 1
	} else {
		stsReplicas = s.instance.Spec.Replicas + 1
	}

	for i := 0; i < int(s.instance.Spec.Shards); i++ {
		labels := k8sutils.CopyLabels(s.labels)
		stsName := fmt.Sprintf("%s-%d", s.labels[redisv1beta1.ClusterNameKey], i)
		labels[redisv1beta1.StatefulSetNameKey] = stsName

		s.Shards[stsName] = NewRedisStatefulset(s.logger, s.k8sclient, s.instance.Namespace, stsReplicas, labels, s.ownerRef, &s.instance.Spec.RedisPod)
		if err := s.Shards[stsName].GetRedisSecret(); err != nil {
			return err
		}
		s.Shards[stsName].InitRedisStatefulSetDefine()
		if err := s.Shards[stsName].GetRedisRuntime(); err != nil {
			return err
		}
	}
	return nil
}

func (s *RedisClusterService) CreateConfigMap() error {
	cm := &corev1.ConfigMap{}
	if err := s.k8sclient.Get(context.TODO(), types.NamespacedName{Name: s.RedisConfigMap.Name, Namespace: s.RedisConfigMap.Namespace}, cm); err != nil {
		if errors.IsNotFound(err) {
			return s.k8sclient.Create(context.TODO(), s.RedisConfigMap)
		}
		return err
	}
	return nil
}

func (s *RedisClusterService) CreateOrUpdateRedisStatefulset() (bool, error) {
	continueRun := true
	for _, shard := range s.Shards {
		if shard.RedisStatefulSetRuntime == nil {
			continueRun = false
			if err := shard.CreateRedisStatefulSet(); err != nil {
				return continueRun, err
			}
			continue
		}

		if !continueRun {
			time.Sleep(3 * time.Second)
			return continueRun, nil
		}

		patchResult, err := shard.CompareStatefulSet()
		if err != nil {
			s.logger.Error(err, "对比失败")
			return false, err
		}

		if !patchResult.IsEmpty() {
			s.logger.Info("对比不为空, 进行更新")
			s.logger.Info(string(patchResult.Patch))
			continueRun = false
			if err := shard.UpdateRedisStatefulSet(); err != nil {
				return continueRun, err
			}
			continue
		}
	}
	return true, nil
}

func (s *RedisClusterService) CreateRedisPodDisruptionBudget() error {
	for _, shard := range s.Shards {
		if err := shard.CreateRedisPodDisruptionBudgetIfNotExits(); err != nil {
			return err
		}
	}
	return nil
}

func (s *RedisClusterService) CreateRedisServiceIfNotExit() error {
	svc := &corev1.Service{}
	if err := s.k8sclient.Get(context.TODO(), types.NamespacedName{Name: s.instance.Spec.ServiceName, Namespace: s.instance.Namespace}, svc); err != nil {
		if errors.IsNotFound(err) {
			return s.CreateRedisService()
		}
		return err
	}
	return nil
}

func (s *RedisClusterService) CreateRedisService() error {
	var ports []corev1.ServicePort

	if s.instance.Spec.RedisConfig.Port != 0 {
		clientPort := corev1.ServicePort{Name: "client", Port: s.instance.Spec.RedisConfig.Port}
		gossipPort := corev1.ServicePort{Name: "gossip", Port: s.instance.Spec.RedisConfig.Port + 10000}
		ports = append(ports, clientPort, gossipPort)
	}

	if s.instance.Spec.RedisExporter != nil {
		isSet := false
		if s.instance.Spec.RedisExporter.Env != nil {
			for _, env := range s.instance.Spec.RedisExporter.Env {
				if env.Name == "REDIS_EXPORTER_WEB_LISTEN_ADDRESS" {
					ipPort := strings.Split(env.Value, ":")
					if len(ipPort) == 2 {
						if port, err := strconv.Atoi(ipPort[1]); err != nil {
							s.logger.Error(err, "端口类型转换失败")
						} else {
							promPort := corev1.ServicePort{Name: "prom-http", Port: int32(port)}
							ports = append(ports, promPort)
							isSet = true
						}
					}
				}
			}
		}
		if !isSet {
			promPort := corev1.ServicePort{Name: "prom-http", Port: 9121}
			ports = append(ports, promPort)
		}
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          s.labels,
			Name:            s.instance.Spec.ServiceName,
			Namespace:       s.instance.Namespace,
			OwnerReferences: s.ownerRef,
		},
		Spec: corev1.ServiceSpec{
			Ports:    ports,
			Selector: s.labels,
		},
	}

	return s.k8sclient.Create(context.TODO(), svc)
}

func (s *RedisClusterService) FixTerminatingPods() (bool, error) {
	continueRun := true
	for _, shard := range s.Shards {
		if b, err := shard.FixTerminatingPods(); err != nil {
			return b, err
		} else if !b {
			continueRun = false
		}
	}
	return continueRun, nil
}

func (s *RedisClusterService) InitCreateRedisClusterNodes() error {
	for _, shard := range s.Shards {
		if err := shard.InitCreateRedisClusterNodes(); err != nil {
			return err
		}
	}
	return nil
}

func (s *RedisClusterService) FixFaildRedisNodes() (bool, error) {
	continueRun := true
	for _, shard := range s.Shards {
		if b, err := shard.FixFaildRedisNodes(); err != nil {
			return b, err
		} else if !b {
			continueRun = false
		}
	}
	return continueRun, nil
}

func (s *RedisClusterService) ClusterMeet() (bool, error) {
	continueRun := true
	for _, shard := range s.Shards {
		s.RedisPods = append(s.RedisPods, shard.RedisPods...)
	}

	for _, pod1 := range s.RedisPods {
		for _, pod2 := range s.RedisPods {
			if pod1.RedisPodIP == pod2.RedisPodIP {
				continue
			}

			found := false
			for _, node := range pod1.ClusterNodes.Nodes {
				if node.IP == pod2.RedisPodIP {
					found = true
					break
				}
			}
			if !found {
				continueRun = false
				if err := pod1.ClusterMeet(pod2.RedisPodIP, strconv.Itoa(int(pod2.RedisPort))); err != nil {
					return false, err
				}
			}
		}
	}

	return continueRun, nil
}

func (s *RedisClusterService) ClusterSlotAndSlave() error {
	mastersAll := make(map[string]*RedisPod)
	var mastersHaveSlot []*RedisPod
	var mastersNotSlot []*RedisPod

	for _, shard := range s.Shards {
		master, HaveSlot := shard.GetMaster()
		if master != nil {
			mastersAll[shard.name] = master
			if HaveSlot {
				mastersHaveSlot = append(mastersHaveSlot, master)
			} else {
				mastersNotSlot = append(mastersNotSlot, master)
			}
		}
	}

	// 从库挂在到主库
	for _, shard := range s.Shards {
		if err := shard.AttachingSlavesToMaster(mastersAll[shard.name].RedisPodIP); err != nil {
			return err
		}
	}

	if len(mastersHaveSlot) == 0 {
		// 新建集群, 所有主节点平均分配 slot
		return s.AllocSlots(mastersNotSlot)
	} else if len(mastersHaveSlot) > int(s.instance.Spec.Shards) {
		// TODO: 减少shard, 将多余的节点上的slot分配给其他master
		s.logger.Info("先不搞了, 反正也没人用")
	} else if len(mastersNotSlot) > 0 {
		// 扩容Shard, 将其他master上的slot均到新的主节点, 均衡数据
		return s.RebalancedCluster(mastersHaveSlot, mastersNotSlot)
	}

	return nil
}

func (s *RedisClusterService) CheckRedisNodeNum() error {
	for _, shard := range s.Shards {
		if err := shard.CheckRedisNodeNum(); err != nil {
			return err
		}
	}
	return nil
}

// func (s *RedisClusterService) CheckAndSetRedisStatus() error {
// 	if err := s.RedisStatefulset.RedisPodClassify(); err != nil {
// 		return err
// 	}
// 	return s.RedisStatefulset.SetRedisStatus()
// }

func (s *RedisClusterService) RedisClusterFinalizers() bool {
	// 检查 DeletionTimestamp 以确定对象是否在删除中
	// 存在 DeletionTimestamp 则开始删除, 然后 return ctrl.Result{}, err
	if !s.instance.GetDeletionTimestamp().IsZero() {
		if controllerutil.ContainsFinalizer(s.instance, redisv1beta1.RedisClusterFinalizer) {
			if err := s.DeleteRedisCluster(); err != nil {
				s.logger.Error(err, "RedisClusterFinalizers", "deleteResources", "faield")
				return false
			}

			controllerutil.RemoveFinalizer(s.instance, redisv1beta1.RedisClusterFinalizer)
			if err := s.k8sclient.Update(context.TODO(), s.instance); err != nil {
				s.logger.Error(err, "Could not remove finalizer "+redisv1beta1.RedisClusterFinalizer)
				return false
			}
		}
		return false
	}

	// 没有 DeletionTimestamp 则添加 Finalizer 标签后继续后续逻辑
	if !controllerutil.ContainsFinalizer(s.instance, redisv1beta1.RedisClusterFinalizer) {
		controllerutil.AddFinalizer(s.instance, redisv1beta1.RedisClusterFinalizer)
		if err := s.k8sclient.Update(context.TODO(), s.instance); err != nil {
			s.logger.Error(err, "Could not add finalizer"+redisv1beta1.RedisClusterFinalizer)
			return false
		}
		return false
	}
	return true
}

func (s *RedisClusterService) DeleteRedisCluster() error {
	// TODO: 删除 redidcluster 中的所有 statefulset, service, pb 等逻辑, statefulset 需要设置副本为1然后慢慢删除
	// TODO: 删除 redidcluster 中所有 service
	return nil
}

func (s *RedisClusterService) AllocSlots(mastersNotSlot []*RedisPod) error {
	mastersNum := len(mastersNotSlot)
	clusterHashSlots := int(v1beta1.DefaultHashMaxSlots + 1)
	slotsPerNode := float64(clusterHashSlots) / float64(mastersNum)
	first := 0
	cursor := 0.0
	for index, pod := range mastersNotSlot {
		last := redisutils.Round(cursor + slotsPerNode - 1)
		if last > clusterHashSlots || index == mastersNum-1 {
			last = clusterHashSlots - 1
		}

		if last < first {
			last = first
		}

		slots := redisutils.BuildSlotSlice(redisutils.Slot(first), redisutils.Slot(last))
		first = last + 1
		cursor += slotsPerNode
		if err := pod.AddSlots(slots); err != nil {
			return err
		}
	}
	return nil
}

// RebalancedCluster rebalanced a redis cluster.
func (s *RedisClusterService) RebalancedCluster(mastersHaveSlot []*RedisPod, mastersNotSlot []*RedisPod) error {
	var masterAll []*RedisPod
	masterAll = append(masterAll, mastersHaveSlot...)
	masterAll = append(masterAll, mastersNotSlot...)
	nbNode := len(masterAll)
	for _, pod := range masterAll {
		expected := int(float64(v1beta1.DefaultHashMaxSlots+1) / float64(nbNode))
		pod.ClusterNodes.MyNode.SetBalance(len(pod.ClusterNodes.MyNode.Slots) - expected)
	}

	totalBalance := 0
	for _, pod := range mastersNotSlot {
		totalBalance += pod.ClusterNodes.MyNode.Balance()
	}

	for totalBalance > 0 {
		for _, pod := range mastersNotSlot {
			if pod.ClusterNodes.MyNode.Balance() < 0 && totalBalance > 0 {
				b := pod.ClusterNodes.MyNode.Balance() - 1
				pod.ClusterNodes.MyNode.SetBalance(b)
				totalBalance -= 1
			}
		}
	}

	// Sort nodes by their slots balance.
	length := len(masterAll)
	for i := 0; i < length-1; i++ {
		flag := true
		for j := 0; j < length-1-i; j++ {
			if masterAll[j].ClusterNodes.MyNode.Balance() > masterAll[j+1].ClusterNodes.MyNode.Balance() {
				masterAll[j], masterAll[j+1] = masterAll[j+1], masterAll[j]
				flag = false
			}
		}
		if flag {
			break
		}
	}

	s.logger.Info(">>> rebalancing", "nodeNum", nbNode)

	dstIdx := 0
	srcIdx := len(masterAll) - 1

	for dstIdx < srcIdx {
		dst := masterAll[dstIdx]
		src := masterAll[srcIdx]

		var numSlots float64
		if math.Abs(float64(dst.ClusterNodes.MyNode.Balance())) < math.Abs(float64(src.ClusterNodes.MyNode.Balance())) {
			numSlots = math.Abs(float64(dst.ClusterNodes.MyNode.Balance()))
		} else {
			numSlots = math.Abs(float64(src.ClusterNodes.MyNode.Balance()))
		}

		if numSlots > 0 {
			s.logger.Info(fmt.Sprintf("Moving %f slots from %s to %s", numSlots, src.RedisPodIP, dst.RedisPodIP))
			srcs := redisutils.Nodes{src.ClusterNodes.MyNode}
			reshardTable := redisutils.ComputeReshardTable(srcs, int(numSlots))
			if len(reshardTable) != int(numSlots) {
				s.logger.Error(nil, "*** Assertion failed: Reshard table != number of slots", "table", len(reshardTable), "slots", numSlots)
			}
			for _, e := range reshardTable {
				if err := s.moveSlot(src, dst, e); err != nil {
					return err
				}

			}
		}

		// Update nodes balance.
		s.logger.V(4).Info("balance", "dst", dst.ClusterNodes.MyNode.Balance(), "src", src.ClusterNodes.MyNode.Balance(), "slots", numSlots)
		dst.ClusterNodes.MyNode.SetBalance(dst.ClusterNodes.MyNode.Balance() + int(numSlots))
		src.ClusterNodes.MyNode.SetBalance(src.ClusterNodes.MyNode.Balance() - int(numSlots))
		if dst.ClusterNodes.MyNode.Balance() == 0 {
			dstIdx += 1
		}
		if src.ClusterNodes.MyNode.Balance() == 0 {
			srcIdx -= 1
		}
	}

	return nil
}

func (s *RedisClusterService) moveSlot(src *RedisPod, dst *RedisPod, slot redisutils.Slot) error {
	if err := dst.RedisClient.ClusterSetslotImporting(slot, src.ClusterNodes.MyNode.ID); err != nil {
		return err
	}
	if err := src.RedisClient.ClusterSetslotMigrating(slot, dst.ClusterNodes.MyNode.ID); err != nil {
		return err
	}

	keyCount := 0
	for {
		keys, err := src.RedisClient.ClusterGetkeysinslot(slot, 10)
		if err != nil {
			s.logger.Error(err, "wrong returned format for CLUSTER GETKEYSINSLOT")
			return err
		}

		keyCount += len(keys)
		if len(keys) == 0 {
			break
		}

		if err := src.RedisClient.ClusterMigrate(dst.RedisPodIP, strconv.Itoa(int(dst.RedisPort)), keys, strconv.Itoa(30000)); err != nil {
			return err
		}
	}

	if err := dst.RedisClient.ClusterSetslotNode(slot, dst.ClusterNodes.MyNode.ID); err != nil {
		return err
	}
	if err := src.RedisClient.ClusterSetslotNode(slot, dst.ClusterNodes.MyNode.ID); err != nil {
		return err
	}
	return nil
}
