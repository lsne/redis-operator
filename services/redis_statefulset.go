// Created by lsne on 2022-11-06 21:48:10

package services

import (
	"context"
	"crypto/tls"
	"fmt"
	"path"
	redisv1beta1 "redis-operator/api/v1beta1"
	"redis-operator/utils/k8sutils"
	"redis-operator/utils/redisutils"
	"strconv"
	"time"

	policyv1 "k8s.io/api/policy/v1"

	"github.com/banzaicloud/k8s-objectmatcher/patch"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/intstr"

	corev1 "k8s.io/api/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type RedisStatefulset struct {
	logger                  logr.Logger
	k8sclient               client.Client
	namespace               string
	name                    string
	replicas                int32
	labels                  map[string]string
	ownerRef                []metav1.OwnerReference
	account                 *corev1.Secret
	tlsSecret               *corev1.Secret
	tls                     bool
	pb                      *policyv1.PodDisruptionBudget
	rsPodsDef               *redisv1beta1.RedisPod
	RedisStatefulSetDefine  *appsv1.StatefulSet
	RedisStatefulSetRuntime *appsv1.StatefulSet
	RedisPods               []*RedisPod
	// RedisPods               *corev1.PodList
	RedisMasterPod  *RedisPod
	RedisSlavePod   []*RedisPod
	RedisNotRolePod []*RedisPod
}

func NewRedisStatefulset(logger logr.Logger, client client.Client, namespace string, replicas int32, labels map[string]string, ownerRef []metav1.OwnerReference, redisPod *redisv1beta1.RedisPod) *RedisStatefulset {
	minAvailable := &intstr.IntOrString{Type: intstr.Int, IntVal: 1}
	pb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          labels,
			Name:            labels[redisv1beta1.StatefulSetNameKey],
			Namespace:       namespace,
			OwnerReferences: ownerRef,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
		},
	}

	return &RedisStatefulset{
		logger:    logger,
		k8sclient: client,
		namespace: namespace,
		name:      labels[redisv1beta1.StatefulSetNameKey],
		replicas:  replicas,
		labels:    labels,
		ownerRef:  ownerRef,
		pb:        pb,
		rsPodsDef: redisPod,
	}
}

func (s *RedisStatefulset) GetRedisSecret() error {
	if s.rsPodsDef.TLSSecret != nil && s.rsPodsDef.TLSSecret.Name != "" {
		s.tls = true
	}

	if s.tls && s.rsPodsDef.TLSSecret.CreateIfNotExists {
		if err := s.CreateRedisTlsSecret(); err != nil {
			return err
		}
	}

	if s.tls {
		if err := s.k8sclient.Get(context.TODO(), types.NamespacedName{Namespace: s.namespace, Name: s.rsPodsDef.TLSSecret.Name}, s.tlsSecret); err != nil {
			if errors.IsNotFound(err) {
				if err := s.CreateRedisTlsSecret(); err != nil {
					return err
				}
				return nil
			}
			return err
		}
	}

	if s.rsPodsDef.AccountSecret != nil && s.rsPodsDef.AccountSecret.Name != "" {
		if err := s.k8sclient.Get(context.TODO(), types.NamespacedName{Namespace: s.namespace, Name: s.rsPodsDef.AccountSecret.Name}, s.account); err != nil {
			return err
		}
	}

	return nil
}

// TODO: 实现自动创建 tls cert 创建 k8s secret, 并且将 secret 赋值到 s.tlsSecret
func (s *RedisStatefulset) CreateRedisTlsSecret() error {
	return nil
}

func (s *RedisStatefulset) GetRedisRuntime() error {
	statefulSet := &appsv1.StatefulSet{}
	if err := s.k8sclient.Get(context.TODO(), types.NamespacedName{Name: s.name, Namespace: s.namespace}, statefulSet); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	s.RedisStatefulSetRuntime = statefulSet

	pods := &corev1.PodList{}
	if err := s.k8sclient.List(context.TODO(), pods, client.InNamespace(s.namespace), client.MatchingLabels(s.labels)); err != nil {
		return err
	}

	var username string
	var password string
	var cert *tls.Config
	var err error

	if s.account != nil {
		if _, ok := s.account.Data["root-username"]; ok {
			username = string(s.account.Data["root-username"])
		}
		if _, ok := s.account.Data["root-password"]; ok {
			password = string(s.account.Data["root-password"])
		}
	}

	port := s.rsPodsDef.RedisConfig.Port
	if s.tls {
		if cert, err = redisutils.GetRedisCert(s.tlsSecret.Data["ca.crt"], s.tlsSecret.Data["redis.crt"], s.tlsSecret.Data["redis.key"], ""); err != nil {
			return err
		}
		port = s.rsPodsDef.RedisConfig.TlsPort
	}

	for _, pod := range pods.Items {
		rp := NewRedisPod(s.logger, s.k8sclient, pod, port, username, password, cert, s.rsPodsDef.RedisConfig.RenameCommand)
		s.RedisPods = append(s.RedisPods, rp)
	}
	return nil
}

// 获取nodes
func (s *RedisStatefulset) InitCreateRedisClusterNodes() error {
	for _, pod := range s.RedisPods {
		if err := pod.GetClusterNodes(); err != nil {
			return err
		}
	}
	return nil
}

func (s *RedisStatefulset) InitRedisStatefulSetDefine() error {
	var containers []corev1.Container

	labels := k8sutils.CopyLabels(s.labels)
	labels[redisv1beta1.RedisRoleKey] = ""

	volumes := s.getRedisStatefulSetVolumes()
	volumeClaimTemplate := s.getRedisStatefulSetVolumeClaimTemplates()
	RedisvolumeMounts := s.getRedisStatefulSetContainerVolumeMounts(volumes, volumeClaimTemplate)
	ExportervolumeMounts := s.getExporterStatefulSetContainerVolumeMounts(volumes)

	redisPodCmd := []string{"sh", path.Join(redisv1beta1.RedisBinVolumeMountPath, redisv1beta1.RedisStartupScript)}
	readinessProbe := &corev1.Probe{
		InitialDelaySeconds: redisv1beta1.RedisHealthCheckInitialDelaySeconds,
		TimeoutSeconds:      redisv1beta1.RedisHealthCheckTimeoutSeconds,
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sh", path.Join(redisv1beta1.RedisBinVolumeMountPath, redisv1beta1.RedisHealthCheckScript)},
			},
		},
	}
	livenessProbe := &corev1.Probe{
		InitialDelaySeconds: redisv1beta1.RedisHealthCheckInitialDelaySeconds,
		TimeoutSeconds:      redisv1beta1.RedisHealthCheckTimeoutSeconds,
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sh", path.Join(redisv1beta1.RedisBinVolumeMountPath, redisv1beta1.RedisHealthCheckScript)},
			},
		},
	}
	lifecycle := &corev1.Lifecycle{
		PreStop: &corev1.LifecycleHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sh", path.Join(redisv1beta1.RedisBinVolumeMountPath, redisv1beta1.RedisShutdownScript)},
			},
		},
	}

	port := s.rsPodsDef.RedisConfig.Port
	if port == 0 {
		port = s.rsPodsDef.RedisConfig.TlsPort
	}

	gossipPort := port + 10000

	ports := []corev1.ContainerPort{{Name: "client", ContainerPort: port, Protocol: corev1.ProtocolTCP}, {Name: "gossip", ContainerPort: gossipPort, Protocol: corev1.ProtocolTCP}}

	redidEnv, err := s.initRedisEnv()
	if err != nil {
		return err
	}

	exporterEnv, err := s.initExporterEnv()
	if err != nil {
		return err
	}

	c1 := k8sutils.GenerateContainer("redis", s.rsPodsDef.Image, s.rsPodsDef.ImagePullPolicy, s.rsPodsDef.ContainerSecurityContext, ports, redidEnv, redisPodCmd, nil, readinessProbe, livenessProbe, lifecycle, RedisvolumeMounts, s.rsPodsDef.Resources)
	containers = append(containers, c1)

	if s.rsPodsDef.RedisExporter != nil {
		c2 := k8sutils.GenerateContainer("exporter", s.rsPodsDef.RedisExporter.Image, s.rsPodsDef.RedisExporter.ImagePullPolicy, s.rsPodsDef.RedisExporter.SecurityContext, nil, exporterEnv, nil, s.rsPodsDef.RedisExporter.Args, nil, nil, nil, ExportervolumeMounts, s.rsPodsDef.RedisExporter.Resources)
		containers = append(containers, c2)
	}

	// var annotations = make(map[string]string)
	// if s.podInfo.RedisPod != nil && s.podInfo.RedisPod.Annotations != nil {
	// 	annotations = s.podInfo.RedisPod.Annotations
	// }
	podObjectMeta := k8sutils.GeneratePODObjectMeta(labels, s.rsPodsDef.Annotations)
	podSpec := k8sutils.GeneratePodSpec(containers, s.rsPodsDef.ImagePullSecrets, s.rsPodsDef.NodeSelector, volumes, s.rsPodsDef.PodSecurityContext, s.rsPodsDef.PriorityClassName, s.rsPodsDef.Affinity)

	stsTypeMeta := k8sutils.GenerateTypeMeta("StatefulSet", "apps/v1")
	stsObjectMeta := k8sutils.GenerateSTSObjectMeta(s.labels[redisv1beta1.StatefulSetNameKey], s.namespace, s.labels, s.rsPodsDef.Annotations, s.ownerRef)
	stsSpec := k8sutils.GenerateStatefulSetSpec("svc-"+s.labels[redisv1beta1.StatefulSetNameKey], &s.replicas, s.labels, podObjectMeta, podSpec, volumeClaimTemplate)
	s.RedisStatefulSetDefine = k8sutils.GenerateStatefulSets(stsTypeMeta, stsObjectMeta, stsSpec)
	return nil
}

func (s *RedisStatefulset) getRedisStatefulSetVolumes() []corev1.Volume {
	var volumes []corev1.Volume
	// config 用环境变量, 不挂载
	// 存储有 pvctemp 生成, 不用挂载

	//  mount cert
	if s.rsPodsDef.TLSSecret != nil && s.rsPodsDef.TLSSecret.Name != "" {
		volumes = append(volumes, k8sutils.GenerateVolumeFromSecret(redisv1beta1.RedisCertVolumeName, s.rsPodsDef.TLSSecret.Name))
	}

	//  mount script
	volumes = append(volumes, k8sutils.GenerateVolumeFromConfigMap(redisv1beta1.RedisBinVolumeName, s.labels[redisv1beta1.ClusterNameKey]+redisv1beta1.RedisConfigMapNameSuffix))
	// mount labels
	volumes = append(volumes, k8sutils.GenerateVolumeFromDownwardAPI(redisv1beta1.RedisLabelsVolumeName, []k8sutils.DownwardAPIItem{{Filename: redisv1beta1.RedisLabelsVolumeMountFilename, Fieldname: redisv1beta1.RedisLabelsVolumeMountFieldname}}))

	return volumes
}

func (s *RedisStatefulset) getRedisStatefulSetVolumeClaimTemplates() []corev1.PersistentVolumeClaim {
	if s.rsPodsDef.RedisPVCStorage != nil {
		return []corev1.PersistentVolumeClaim{k8sutils.GeneratePersistentVolumeClaimTemplates(redisv1beta1.RedisDataVolumeName, s.labels, *s.rsPodsDef.RedisPVCStorage, s.ownerRef)}
	}
	return nil
}

func (s *RedisStatefulset) getRedisStatefulSetContainerVolumeMounts(volumes []corev1.Volume, pvcs []corev1.PersistentVolumeClaim) []corev1.VolumeMount {
	var vmounts []corev1.VolumeMount
	for _, volume := range volumes {
		switch volume.Name {
		case redisv1beta1.RedisBinVolumeName:
			vmounts = append(vmounts, corev1.VolumeMount{Name: volume.Name, MountPath: redisv1beta1.RedisBinVolumeMountPath})
		case redisv1beta1.RedisConfigVolumeName:
			vmounts = append(vmounts, corev1.VolumeMount{Name: volume.Name, MountPath: redisv1beta1.RedisConfigVolumeMountPath})
		case redisv1beta1.RedisDataVolumeName: // 应该没用
			vmounts = append(vmounts, corev1.VolumeMount{Name: volume.Name, MountPath: redisv1beta1.RedisDataVolumeMountPath})
		case redisv1beta1.RedisCertVolumeName:
			vmounts = append(vmounts, corev1.VolumeMount{Name: volume.Name, MountPath: redisv1beta1.RedisCertVolumeMountPath})
		case redisv1beta1.RedisLabelsVolumeName:
			vmounts = append(vmounts, corev1.VolumeMount{Name: volume.Name, MountPath: redisv1beta1.RedisLabelsVolumeMountPath})
		}
	}

	for _, pvc := range pvcs {
		switch pvc.ObjectMeta.Name {
		case redisv1beta1.RedisDataVolumeName:
			vmounts = append(vmounts, corev1.VolumeMount{Name: pvc.ObjectMeta.Name, MountPath: redisv1beta1.RedisDataVolumeMountPath})
		}
	}
	return vmounts
}

func (s *RedisStatefulset) getExporterStatefulSetContainerVolumeMounts(volumes []corev1.Volume) []corev1.VolumeMount {
	var vmounts []corev1.VolumeMount
	for _, volume := range volumes {
		switch volume.Name {
		case redisv1beta1.RedisCertVolumeName:
			vmounts = append(vmounts, corev1.VolumeMount{Name: volume.Name, MountPath: redisv1beta1.RedisCertVolumeMountPath})
		}
	}
	return vmounts
}

func (s *RedisStatefulset) initRedisEnv() ([]corev1.EnvVar, error) {
	envs := s.rsPodsDef.Env
	envs = append(envs,
		corev1.EnvVar{Name: "REDIS_CONFIG_ACTIVEREHASHING", Value: s.rsPodsDef.RedisConfig.Activerehashing},
		corev1.EnvVar{Name: "REDIS_CONFIG_ACTIVEDEFRAG", Value: s.rsPodsDef.RedisConfig.Activedefrag},
		corev1.EnvVar{Name: "REDIS_CONFIG_ACTIVE_DEFRAG_CYCLE_MIN", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.ActiveDefragCycleMin, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_ACTIVE_DEFRAG_CYCLE_MAX", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.ActiveDefragCycleMax, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_ACTIVE_DEFRAG_IGNORE_BYTES", Value: s.rsPodsDef.RedisConfig.ActiveDefragIgnoreBytes},
		corev1.EnvVar{Name: "REDIS_CONFIG_ACTIVE_DEFRAG_THRESHOLD_LOWER", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.ActiveDefragThresholdLower, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_ACTIVE_DEFRAG_THRESHOLD_UPPER", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.ActiveDefragThresholdUpper, 10)},

		corev1.EnvVar{Name: "REDIS_CONFIG_APPENDONLY", Value: s.rsPodsDef.RedisConfig.Appendonly},
		corev1.EnvVar{Name: "REDIS_CONFIG_APPENDFSYNC", Value: s.rsPodsDef.RedisConfig.Appendfsync},
		corev1.EnvVar{Name: "REDIS_CONFIG_AOF_LOAD_TRUNCATED", Value: s.rsPodsDef.RedisConfig.AofLoadTruncated},
		corev1.EnvVar{Name: "REDIS_CONFIG_AOF_USE_RDB_PREAMBLE", Value: s.rsPodsDef.RedisConfig.AofUseRdbPreamble},
		corev1.EnvVar{Name: "REDIS_CONFIG_NO_APPENDFSYNC_ON_REWRITE", Value: s.rsPodsDef.RedisConfig.NoAppendfsyncOnRewrite},
		corev1.EnvVar{Name: "REDIS_CONFIG_AUTO_AOF_REWRITE_PERCENTAGE", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.AutoAofRewritePercentage, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_AUTO_AOF_REWRITE_MIN_SIZE", Value: s.rsPodsDef.RedisConfig.AutoAofRewriteMinSize},

		corev1.EnvVar{Name: "REDIS_CONFIG_LOGLEVEL", Value: s.rsPodsDef.RedisConfig.Loglevel},
		corev1.EnvVar{Name: "REDIS_CONFIG_PORT", Value: strconv.Itoa(int(s.rsPodsDef.RedisConfig.Port))},
		corev1.EnvVar{Name: "REDIS_CONFIG_DATABASES", Value: strconv.Itoa(int(s.rsPodsDef.RedisConfig.Databases))},
		corev1.EnvVar{Name: "REDIS_CONFIG_RDBCHECKSUM", Value: s.rsPodsDef.RedisConfig.Rdbchecksum},
		corev1.EnvVar{Name: "REDIS_CONFIG_RDBCOMPRESSION", Value: s.rsPodsDef.RedisConfig.Rdbcompression},
		corev1.EnvVar{Name: "REDIS_CONFIG_STOP_WRITES_ON_BGSAVE_ERROR", Value: s.rsPodsDef.RedisConfig.StopWritesOnBgsaveError},
		corev1.EnvVar{Name: "REDIS_CONFIG_TIMEOUT", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.Timeout, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_MAXCLIENTS", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.Maxclients, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_MAXMEMORY", Value: s.rsPodsDef.RedisConfig.Maxmemory},
		corev1.EnvVar{Name: "REDIS_CONFIG_MAXMEMORY_POLICY", Value: s.rsPodsDef.RedisConfig.MaxmemoryPolicy},
		corev1.EnvVar{Name: "REDIS_CONFIG_MAXMEMORY_SAMPLES", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.MaxmemorySamples, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_HZ", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.Hz, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_REPL_BACKLOG_SIZE", Value: s.rsPodsDef.RedisConfig.ReplBacklogSize},
		corev1.EnvVar{Name: "REDIS_CONFIG_REPL_BACKLOG_TTL", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.ReplBacklogTtl, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_SLOWLOG_LOG_SLOWER_THAN", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.SlowlogLogSlowerThan, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_SLOWLOG_MAX_LEN", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.SlowlogMaxLen, 10)},

		corev1.EnvVar{Name: "REDIS_CONFIG_TLS_PORT", Value: strconv.Itoa(int(s.rsPodsDef.RedisConfig.TlsPort))},

		corev1.EnvVar{Name: "REDIS_CONFIG_CLUSTER_ENABLED", Value: s.rsPodsDef.RedisConfig.ClusterEnabled},
		corev1.EnvVar{Name: "REDIS_CONFIG_CLUSTER_MIGRATION_BARRIER", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.ClusterMigrationBarrier, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_CLUSTER_REQUIRE_FULL_COVERAGE", Value: s.rsPodsDef.RedisConfig.ClusterRequireFullCoverage},
		corev1.EnvVar{Name: "REDIS_CONFIG_CLUSTER_NODE_TIMEOUT", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.ClusterNodeTimeout, 10)},

		corev1.EnvVar{Name: "REDIS_CONFIG_SET_MAX_INTSET_ENTRIES", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.SetMaxIntsetEntries, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_HASH_MAX_ZIPLIST_ENTRIES", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.HashMaxZiplistEntries, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_HASH_MAX_ZIPLIST_VALUE", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.HashMaxZiplistValue, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_LIST_MAX_ZIPLIST_ENTRIES", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.ListMaxZiplistEntries, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_LIST_MAX_ZIPLIST_VALUE", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.ListMaxZiplistValue, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_ZSET_MAX_ZIPLIST_ENTRIES", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.ZsetMaxZiplistEntries, 10)},
		corev1.EnvVar{Name: "REDIS_CONFIG_ZSET_MAX_ZIPLIST_VALUE", Value: strconv.FormatUint(s.rsPodsDef.RedisConfig.ZsetMaxZiplistValue, 10)},
		corev1.EnvVar{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}})

	if s.rsPodsDef.RedisConfig.ClientOutputBufferLimit.ToString() != "" {
		envs = append(envs, corev1.EnvVar{Name: "REDIS_CONFIG_CLIENT_OUTPUT_BUFFER_LIMIT", Value: s.rsPodsDef.RedisConfig.ClientOutputBufferLimit.ToString()})
	}

	if s.rsPodsDef.RedisConfig.SaveToString() != "" {
		envs = append(envs, corev1.EnvVar{Name: "REDIS_CONFIG_SAVE", Value: s.rsPodsDef.RedisConfig.SaveToString()})
	}

	if s.rsPodsDef.RedisConfig.RenameCommandToString() != "" {
		envs = append(envs, corev1.EnvVar{Name: "REDIS_CONFIG_RENAME_COMMAND", Value: s.rsPodsDef.RedisConfig.RenameCommandToString()})
	}

	if s.rsPodsDef.CustomConfig != "" {
		envs = append(envs, corev1.EnvVar{Name: "REDIS_CUSTOM_CONFIG", Value: s.rsPodsDef.CustomConfig})
	}

	if s.account != nil {
		for key := range s.account.Data {
			switch key {
			case "root-username":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_ROOT_USERNAME", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "root-username"}}})
			case "root-password":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_ROOT_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "root-password"}}})
			case "replication-username":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_REPLICATION_USERNAME", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "replication-username"}}})
			case "replication-password":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_REPLICATION_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "replication-password"}}})
			case "monitor-username":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_MONITOR_USERNAME", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "monitor-username"}}})
			case "monitor-password":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_MONITOR_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "monitor-password"}}})
			case "app01-username":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_APP01_USERNAME", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "app01-username"}}})
			case "app01-password":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_APP01_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "app01-password"}}})
			case "app01-privileges":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_APP01_PRIVILEGES", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "app01-privileges"}}})
			case "app02-username":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_APP02_USERNAME", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "app02-username"}}})
			case "app02-password":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_APP02_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "app02-password"}}})
			case "app02-privileges":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_APP02_PRIVILEGES", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "app02-privileges"}}})
			case "app03-username":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_APP03_USERNAME", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "app03-username"}}})
			case "app03-password":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_APP03_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "app03-password"}}})
			case "app03-privileges":
				envs = append(envs, corev1.EnvVar{Name: "REDIS_APP03_PRIVILEGES", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "app03-privileges"}}})
			}
		}
	}

	return envs, nil
}

func (s *RedisStatefulset) initExporterEnv() ([]corev1.EnvVar, error) {
	var envs []corev1.EnvVar

	if s.rsPodsDef.RedisExporter != nil && s.rsPodsDef.RedisExporter.Env != nil {
		envs = s.rsPodsDef.RedisExporter.Env
	}

	if s.account != nil {
		if _, ok := s.account.Data["monitor-username"]; ok {
			if _, ok := s.account.Data["monitor-password"]; ok {
				envs = append(envs, corev1.EnvVar{Name: "REDIS_USER", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "monitor-username"}}})
				envs = append(envs, corev1.EnvVar{Name: "REDIS_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "monitor-password"}}})
			}
		} else {
			if _, ok := s.account.Data["root-username"]; ok {
				envs = append(envs, corev1.EnvVar{Name: "REDIS_USER", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "root-username"}}})
			}
			if _, ok := s.account.Data["root-password"]; ok {
				envs = append(envs, corev1.EnvVar{Name: "REDIS_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: *s.rsPodsDef.AccountSecret, Key: "root-password"}}})
			}
		}
	}
	if s.rsPodsDef.RedisConfig.ClusterEnabled == "yes" {
		envs = append(envs, corev1.EnvVar{Name: "REDIS_EXPORTER_IS_CLUSTER", Value: "true"})
	}

	if s.rsPodsDef.TLSSecret != nil && s.rsPodsDef.TLSSecret.Name != "" {
		envs = append(envs, corev1.EnvVar{Name: "REDIS_EXPORTER_TLS_CLIENT_KEY_FILE", Value: path.Join(redisv1beta1.RedisCertVolumeMountPath, "redis.key")})
		envs = append(envs, corev1.EnvVar{Name: "REDIS_EXPORTER_TLS_CLIENT_CERT_FILE", Value: path.Join(redisv1beta1.RedisCertVolumeMountPath, "redis.crt")})
		envs = append(envs, corev1.EnvVar{Name: "REDIS_EXPORTER_TLS_CA_CERT_FILE", Value: path.Join(redisv1beta1.RedisCertVolumeMountPath, "ca.crt")})
		envs = append(envs, corev1.EnvVar{Name: "REDIS_EXPORTER_SKIP_TLS_VERIFICATION", Value: "true"})
	}

	return envs, nil
}

func (s *RedisStatefulset) CompareStatefulSet() (*patch.PatchResult, error) {
	s.RedisStatefulSetDefine.ResourceVersion = s.RedisStatefulSetRuntime.ResourceVersion
	s.RedisStatefulSetDefine.CreationTimestamp = s.RedisStatefulSetRuntime.CreationTimestamp
	s.RedisStatefulSetDefine.ManagedFields = s.RedisStatefulSetRuntime.ManagedFields
	return patch.DefaultPatchMaker.Calculate(s.RedisStatefulSetRuntime, s.RedisStatefulSetDefine,
		patch.IgnoreStatusFields(),
		patch.IgnoreVolumeClaimTemplateTypeMetaAndStatus(),
		patch.IgnoreField("spec.template.spec.containers[0].env"), // TODO: patch.IgnoreField 只能忽略最外层字段, 嵌套字段还需要想办法. 或者一层一层推上来。先比较容器忽略env。再比较pod忽略spec, 再比较tempspec 忽略 pod, 再比较。。。
		// patch.IgnoreField("spec.template.metadata"),   //好像修改标签不影响, 注释这一行, 修改标签之后对比也是一样的
		patch.IgnoreField("metadata"),
		patch.IgnoreField("kind"),
		patch.IgnoreField("apiVersion"))
}

func (s *RedisStatefulset) FixTerminatingPods() (bool, error) {
	continueRun := true
	var err error

	for _, pod := range s.RedisPods {
		if pod.Terminating() {
			continueRun = false
			if err1 := pod.Delete(true); err1 != nil {
				err = err1
			}
		}
	}
	return continueRun, err
}

func (s *RedisStatefulset) FixFaildRedisNodes() (bool, error) {
	continueRun := true
	var err error

	for _, pod := range s.RedisPods {
		for _, node := range pod.ClusterNodes.Nodes {
			if len(node.FailStatus) > 0 {
				continueRun = false
				del := true
				for _, pd := range s.RedisPods {
					if pd.RedisPodIP == node.IP {
						del = false
						break
					}
				}
				if del {
					if err := pod.ClusterForget(node.ID); err != nil {
						return false, err
					}
				}
			}
		}
	}
	return continueRun, err
}

func (s *RedisStatefulset) CheckRedisNodeNum() error {
	if s.RedisStatefulSetRuntime == nil {
		return fmt.Errorf("status is nil")
	}

	if s.RedisStatefulSetRuntime.Status.ReadyReplicas != s.replicas {
		return fmt.Errorf("redis pods are not all ready")
	}

	if s.RedisStatefulSetRuntime.Status.CurrentReplicas != s.replicas {
		return fmt.Errorf("redis pods need to be updated")
	}
	return nil
}

func (s *RedisStatefulset) CreateRedisStatefulSet() error {
	s.logger.Info("create statefulset")
	if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(s.RedisStatefulSetDefine); err != nil {
		s.logger.Error(err, "Unable to patch redis statefulset with comparison object")
		return err
	}
	return s.k8sclient.Create(context.TODO(), s.RedisStatefulSetDefine)
}

func (s *RedisStatefulset) UpdateRedisStatefulSet() error {
	s.logger.Info("update statefulset")
	if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(s.RedisStatefulSetDefine); err != nil {
		s.logger.Error(err, "Unable to patch redis statefulset with comparison object")
		return err
	}
	return s.k8sclient.Update(context.TODO(), s.RedisStatefulSetDefine)
}

func (s *RedisStatefulset) CreateRedisPodDisruptionBudgetIfNotExits() error {
	pb := &policyv1.PodDisruptionBudget{}
	if err := s.k8sclient.Get(context.TODO(), types.NamespacedName{Name: s.name, Namespace: s.namespace}, pb); err != nil {
		if errors.IsNotFound(err) {
			return s.CreateRedisPodDisruptionBudget()
		}
		return err
	}
	return nil
}

func (s *RedisStatefulset) CreateRedisPodDisruptionBudget() error {
	s.logger.Info("create RedisPodDisruptionBudget")
	return s.k8sclient.Create(context.TODO(), s.pb)
}

// DeletePodDisruptionBudget implement the IPodDisruptionBudgetControl.Interface.
func (s *RedisStatefulset) DeletePodDisruptionBudget() error {
	s.logger.Info("delete RedisPodDisruptionBudget")
	return s.k8sclient.Delete(context.TODO(), s.pb)
}

func (s *RedisStatefulset) RedisPodClassify() error {
	var masterRoleNum int
	for _, pod := range s.RedisPods {
		switch pod.GetRole() {
		case "":
			s.RedisNotRolePod = append(s.RedisNotRolePod, pod)
		case redisv1beta1.RedisMasterRole:
			masterRoleNum++
			s.RedisMasterPod = pod
		case redisv1beta1.RedisSlaveRole:
			s.RedisSlavePod = append(s.RedisSlavePod, pod)
		}
	}

	if masterRoleNum > 1 {
		s.logger.Info("存在多个包含 master labels 的pod")
		return fmt.Errorf("存在多个包含 master labels 的pod")
	}

	if s.RedisMasterPod == nil && len(s.RedisSlavePod) == 0 {
		if len(s.RedisNotRolePod) >= 1 {
			s.RedisMasterPod = s.RedisNotRolePod[0]
			s.RedisSlavePod = s.RedisNotRolePod[1:]
			s.RedisNotRolePod = nil
		}
	} else if s.RedisMasterPod == nil {
		s.RedisMasterPod = s.RedisSlavePod[0]
		s.RedisSlavePod = s.RedisSlavePod[1:]
	}

	if s.RedisMasterPod == nil {
		s.logger.Info("找不到主节点")
		return fmt.Errorf("找不到主节点")
	}

	s.RedisSlavePod = append(s.RedisSlavePod, s.RedisNotRolePod...)
	s.RedisNotRolePod = nil

	s.sureMaster()

	if s.RedisMasterPod == nil {
		s.logger.Info("所有节点连接失败")
		return fmt.Errorf("所有节点连接失败")
	}

	return nil
}

func (s *RedisStatefulset) sureMaster() {
	if s.RedisMasterPod == nil {
		return
	}

	if s.CheckMasterStatus() {
		return
	}

	s.SwitchMaster()
	s.sureMaster()
}

func (s *RedisStatefulset) CheckMasterStatus() bool {
	status := false
	for i := 0; i < 20; i++ {
		status = s.RedisMasterPod.RedisStatus()
		if status {
			break
		}
		time.Sleep(3 * time.Second)
	}

	return status
}

func (s *RedisStatefulset) SwitchMaster() {
	if len(s.RedisSlavePod) >= 1 {
		s.RedisMasterPod = s.RedisSlavePod[0]
		s.RedisSlavePod = s.RedisSlavePod[1:]
	} else {
		s.RedisMasterPod = nil
	}
}

func (s *RedisStatefulset) SetRedisStatus() error {
	mrepl, err := s.RedisMasterPod.GetInfoReplication()
	if err != nil {
		return err
	}

	if mrepl.Role != "master" {
		if err := s.RedisMasterPod.ReplicaOf("no", "one"); err != nil {
			return err
		}
	}

	if s.RedisMasterPod.GetRole() != redisv1beta1.RedisMasterRole {
		if err := s.RedisMasterPod.SetRole(redisv1beta1.RedisMasterRole); err != nil {
			return err
		}
	}

	for _, slave := range s.RedisSlavePod {
		srepl, serr := slave.GetInfoReplication()
		if serr != nil {
			return serr
		}
		if srepl.Role != "slave" || srepl.MasterLinkStatus == "down" {
			if err := slave.ReplicaOf(s.RedisMasterPod.RedisPodIP, strconv.Itoa(int(s.RedisMasterPod.RedisPort))); err != nil {
				return err
			}
		}
		if slave.GetRole() != redisv1beta1.RedisSlaveRole {
			if err := slave.SetRole(redisv1beta1.RedisSlaveRole); err != nil {
				return err
			}
		}
	}
	return nil
}

// func (s *RedisStatefulset) GetAlivePodIP() []*RedisPod {
// 	var alive []*RedisPod

// 	for _, pod := range s.RedisPods {
// 		if pod.RedisStatus() {
// 			alive = append(alive, pod)
// 		}
// 	}
// 	return alive
// }

func (s *RedisStatefulset) GetMaster() (*RedisPod, bool) {
	for _, pod := range s.RedisPods {
		if pod.ClusterNodes.MyNode.Role == redisv1beta1.RedisMasterRole && len(pod.ClusterNodes.MyNode.Slots) > 0 {
			return pod, true
		}
	}
	for _, pod := range s.RedisPods {
		if pod.ClusterNodes.MyNode.Role == redisv1beta1.RedisMasterRole {
			return pod, false
		}
	}
	return nil, false
}

func (s *RedisStatefulset) AttachingSlavesToMaster(ip string) error {
	var masterID string

	for _, pod := range s.RedisPods {
		if pod.RedisPodIP == ip {
			masterID = pod.ClusterNodes.MyNode.ID
			break
		}
	}

	if masterID == "" {
		return fmt.Errorf("未获取到 masterID")
	}

	for _, pod := range s.RedisPods {
		if pod.ClusterNodes.MyNode.ID == masterID {
			continue
		}

		if pod.ClusterNodes.MyNode.Role == redisv1beta1.RedisSlaveRole && pod.ClusterNodes.MyNode.MasterID == masterID {
			continue
		}

		if err := pod.AttachSlaveToMaster(masterID); err != nil {
			return err
		}
	}
	return nil
}
