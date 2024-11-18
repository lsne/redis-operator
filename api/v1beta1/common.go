// Created by lsne on 2022-11-06 21:38:05

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type RedisPod struct {
	ImagePullSecrets         []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
	Image                    string                        `json:"image,omitempty"`
	ImagePullPolicy          corev1.PullPolicy             `json:"imagePullPolicy,omitempty"`
	Env                      []corev1.EnvVar               `json:"env,omitempty"`
	ContainerSecurityContext *corev1.SecurityContext       `json:"containersecurityContext,omitempty"`
	Resources                corev1.ResourceRequirements   `json:"resources,omitempty"`
	AccountSecret            *corev1.LocalObjectReference  `json:"accountSecret,omitempty"`
	RedisConfig              *RedisConfig                  `json:"redisConfig,omitempty"`
	TLSSecret                *TLSSecret                    `json:"tlsSecret,omitempty"`
	CustomConfig             string                        `json:"customConfig,omitempty"`
	RequiredAntiAffinity     bool                          `json:"requiredAntiAffinity,omitempty"` // TODO: 暂时没用
	Affinity                 *corev1.Affinity              `json:"affinity,omitempty"`
	NodeSelector             map[string]string             `json:"nodeSelector,omitempty"`
	Tolerations              []corev1.Toleration           `json:"tolerations,omitempty"`
	PodSecurityContext       *corev1.PodSecurityContext    `json:"podSecurityContext,omitempty"`
	Annotations              map[string]string             `json:"annotations,omitempty"`
	PriorityClassName        string                        `json:"priorityClassName,omitempty"`
	RedisExporter            *RedisExporter                `json:"redisExporter,omitempty"`
	RedisPVCStorage          *RedisPVCStorage              `json:"redisPVCStorage,omitempty"`
}

func NewRedisPod() RedisPod {
	return RedisPod{
		Image:           DefaultRedisImage,
		ImagePullPolicy: corev1.PullPolicy(DefaultRedisImagePullPolicy),
		RedisConfig:     NewRedisConfig(false),
	}
}

type TLSSecret struct {
	Name              string `json:"name,omitempty"`
	CreateIfNotExists bool   `json:"createIfNotExists,omitempty"`
}

type RedisExporter struct {
	Image            string                        `json:"image"`
	ImagePullPolicy  corev1.PullPolicy             `json:"imagePullPolicy,omitempty"`
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
	Resources        corev1.ResourceRequirements   `json:"resources,omitempty"`
	Env              []corev1.EnvVar               `json:"env,omitempty"`
	Args             []string                      `json:"args,omitempty"`
	SecurityContext  *corev1.SecurityContext       `json:"securityContext,omitempty"`
}

type RedisPVCStorage struct {
	Size        resource.Quantity `json:"size"`
	Type        string            `json:"type"` // 这个字段应该没用
	Class       string            `json:"class"`
	DeleteClaim bool              `json:"deleteClaim,omitempty"`
}

func GenerateRedisReplicaOwnerReferences(rr *RedisReplica) []metav1.OwnerReference {
	isctrl := true
	return []metav1.OwnerReference{
		{
			APIVersion: rr.APIVersion,
			Kind:       rr.Kind,
			Name:       rr.GetName(),
			UID:        rr.GetUID(),
			Controller: &isctrl,
		},
	}
}

func GenerateRedisClusterOwnerReferences(rc *RedisCluster) []metav1.OwnerReference {
	isctrl := true
	return []metav1.OwnerReference{
		{
			APIVersion: rc.APIVersion,
			Kind:       rc.Kind,
			Name:       rc.GetName(),
			UID:        rc.GetUID(),
			Controller: &isctrl,
		},
	}
}
