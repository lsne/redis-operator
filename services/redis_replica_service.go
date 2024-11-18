// Created by lsne on 2022-11-06 21:47:38

package services

import (
	"context"
	"fmt"
	redisv1beta1 "redis-operator/api/v1beta1"
	"redis-operator/utils/k8sutils"
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

type RedisReplicaService struct {
	logger           logr.Logger
	k8sclient        client.Client
	instance         *redisv1beta1.RedisReplica
	ownerRef         []metav1.OwnerReference
	labels           map[string]string
	RedisStatefulset *RedisStatefulset
	RedisService     *corev1.Service
	RedisConfigMap   *corev1.ConfigMap
	RedisSecret      *corev1.Secret // 可能用不到map类型, 单独定义一两个secret类型的变量就好
}

func NewredisReplicaService(logger logr.Logger, client client.Client, instance *redisv1beta1.RedisReplica) *RedisReplicaService {
	r := &RedisReplicaService{
		logger:    logger,
		k8sclient: client,
		instance:  instance,
		ownerRef:  redisv1beta1.GenerateRedisReplicaOwnerReferences(instance),
		labels: map[string]string{
			redisv1beta1.ControllerNameKey: redisv1beta1.ControllerNameValue,
			redisv1beta1.ClusterKindKey:    redisv1beta1.RedisReplicaKind,
			redisv1beta1.ClusterNameKey:    redisv1beta1.RedisReplicaPrefix + instance.Name,
		},
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

func (s *RedisReplicaService) Validator() error {
	return nil
}

func (s *RedisReplicaService) InitRedisStatefulset() error {
	var stsReplicas int32 = 0
	// stsName := redisv1beta1.RedisReplicaPrefix + s.instance.Name
	stsName := s.labels[redisv1beta1.ClusterNameKey]

	if s.instance.Spec.Replicas < 0 {
		stsReplicas = 1
	} else {
		stsReplicas = s.instance.Spec.Replicas + 1
	}

	labels := k8sutils.CopyLabels(s.labels)
	labels[redisv1beta1.StatefulSetNameKey] = stsName

	s.RedisStatefulset = NewRedisStatefulset(s.logger, s.k8sclient, s.instance.Namespace, stsReplicas, labels, s.ownerRef, &s.instance.Spec.RedisPod)
	if err := s.RedisStatefulset.GetRedisSecret(); err != nil {
		return err
	}
	s.RedisStatefulset.InitRedisStatefulSetDefine()
	if err := s.RedisStatefulset.GetRedisRuntime(); err != nil {
		return err
	}
	return nil
}

func (s *RedisReplicaService) CreateConfigMap() error {
	cm := &corev1.ConfigMap{}
	if err := s.k8sclient.Get(context.TODO(), types.NamespacedName{Name: s.RedisConfigMap.Name, Namespace: s.RedisConfigMap.Namespace}, cm); err != nil {
		if errors.IsNotFound(err) {
			return s.k8sclient.Create(context.TODO(), s.RedisConfigMap)
		}
		return err
	}
	return nil
}

func (s *RedisReplicaService) CreateOrUpdateRedisStatefulset() (bool, error) {
	if s.RedisStatefulset.RedisStatefulSetRuntime == nil {
		s.logger.Info("运行时为空")
		err := s.RedisStatefulset.CreateRedisStatefulSet()
		time.Sleep(3 * time.Second)
		return false, err
	}

	patchResult, err := s.RedisStatefulset.CompareStatefulSet()
	if err != nil {
		s.logger.Error(err, "对比失败")
		return false, err
	}

	if !patchResult.IsEmpty() {
		s.logger.Info("对比不为空, 进行更新")
		fmt.Println(string(patchResult.Patch))
		return false, s.RedisStatefulset.UpdateRedisStatefulSet()
	}
	return true, nil
}

func (s *RedisReplicaService) CreateRedisPodDisruptionBudget() error {
	return s.RedisStatefulset.CreateRedisPodDisruptionBudgetIfNotExits()
}

func (s *RedisReplicaService) CreateRedisServiceIfNotExit() error {
	svc := &corev1.Service{}
	if err := s.k8sclient.Get(context.TODO(), types.NamespacedName{Name: s.instance.Spec.ServiceName, Namespace: s.instance.Namespace}, svc); err != nil {
		if errors.IsNotFound(err) {
			return s.CreateRedisService()
		}
		return err
	}
	return nil
}

func (s *RedisReplicaService) CreateRedisService() error {
	var ports []corev1.ServicePort
	if s.instance.Spec.RedisConfig.Port != 0 {
		clientPort := corev1.ServicePort{Name: "client", Port: s.instance.Spec.RedisConfig.Port}
		// gossipPort := corev1.ServicePort{Name: "gossip", Port: s.instance.Spec.RedisConfig.Port + 10000}  // 主从模式没有加10000端口
		// ports = append(ports, clientPort, gossipPort)
		ports = append(ports, clientPort)
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

	labels := k8sutils.CopyLabels(s.labels)
	labels[redisv1beta1.RedisRoleKey] = redisv1beta1.RedisMasterRole

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          labels,
			Name:            s.instance.Spec.ServiceName,
			Namespace:       s.instance.Namespace,
			OwnerReferences: s.ownerRef,
		},
		Spec: corev1.ServiceSpec{
			Ports:    ports,
			Selector: labels,
		},
	}

	return s.k8sclient.Create(context.TODO(), svc)
}

func (s *RedisReplicaService) FixTerminatingPods() (bool, error) {
	return s.RedisStatefulset.FixTerminatingPods()
}

func (s *RedisReplicaService) CheckRedisNodeNum() error {
	return s.RedisStatefulset.CheckRedisNodeNum()
}

func (s *RedisReplicaService) CheckAndSetRedisStatus() error {
	if err := s.RedisStatefulset.RedisPodClassify(); err != nil {
		return err
	}
	return s.RedisStatefulset.SetRedisStatus()
}

func (s *RedisReplicaService) RedisReplicaFinalizers() bool {
	// 检查 DeletionTimestamp 以确定对象是否在删除中
	// 存在 DeletionTimestamp 则开始删除, 然后 return ctrl.Result{}, err
	if !s.instance.GetDeletionTimestamp().IsZero() {
		if controllerutil.ContainsFinalizer(s.instance, redisv1beta1.RedisReplicaFinalizer) {
			if err := s.DeleteRedisReplica(); err != nil {
				s.logger.Error(err, "RedisReplicaFinalizers", "deleteResources", "faield")
				return false
			}

			controllerutil.RemoveFinalizer(s.instance, redisv1beta1.RedisReplicaFinalizer)
			if err := s.k8sclient.Update(context.TODO(), s.instance); err != nil {
				s.logger.Error(err, "Could not remove finalizer "+redisv1beta1.RedisReplicaFinalizer)
				return false
			}
		}
		return false
	}

	// 没有 DeletionTimestamp 则添加 Finalizer 标签后继续后续逻辑
	if !controllerutil.ContainsFinalizer(s.instance, redisv1beta1.RedisReplicaFinalizer) {
		controllerutil.AddFinalizer(s.instance, redisv1beta1.RedisReplicaFinalizer)
		if err := s.k8sclient.Update(context.TODO(), s.instance); err != nil {
			s.logger.Error(err, "Could not add finalizer"+redisv1beta1.RedisReplicaFinalizer)
			return false
		}
		return false
	}
	return true
}

func (s *RedisReplicaService) DeleteRedisReplica() error {
	// TODO: 删除 redisreplica 中的所有 statefulset, service, pb 等逻辑, statefulset 需要设置副本为1然后慢慢删除
	// TODO: 删除 redisreplica 中所有 service
	return nil
}
