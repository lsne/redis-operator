/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RedisReplicaSpec defines the desired state of RedisReplica
type RedisReplicaSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Foo is an example field of RedisReplica. Edit redisreplica_types.go to remove/update
	ServiceName string `json:"serviceName,omitempty"`
	Replicas    int32  `json:"replicas,omitempty"`
	RedisPod    `json:"redisPod,inline"`
}

func (r *RedisReplicaSpec) InitArgs() {
	if r.Replicas < 0 {
		r.Replicas = 0
	}
	if r.Replicas > 99 {
		r.Replicas = 99
	}

	if r.Image == "" {
		r.Image = DefaultRedisImage
	}

	if r.ImagePullPolicy == "" {
		r.ImagePullPolicy = corev1.PullPolicy(DefaultRedisImagePullPolicy)
	}

	tls := false
	if r.TLSSecret != nil && r.TLSSecret.Name != "" {
		tls = true
	}

	if r.RedisConfig == nil {
		r.RedisConfig = NewRedisConfig(tls)
	} else {
		r.RedisConfig.InitArgs(tls)
	}
}

// RedisReplicaStatus defines the observed state of RedisReplica
type RedisReplicaStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	Phase  string `json:"phase,omitempty"`
	Reason string `json:"reason,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// RedisReplica is the Schema for the redisreplicas API
type RedisReplica struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedisReplicaSpec   `json:"spec,omitempty"`
	Status RedisReplicaStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RedisReplicaList contains a list of RedisReplica
type RedisReplicaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RedisReplica `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RedisReplica{}, &RedisReplicaList{})
}
