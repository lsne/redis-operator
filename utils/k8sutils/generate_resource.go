// Created by lsne on 2022-11-06 21:49:28

package k8sutils

import (
	"redis-operator/api/v1beta1"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func GenerateTypeMeta(kind string, apiVersion string) metav1.TypeMeta {
	return metav1.TypeMeta{
		Kind:       kind,
		APIVersion: apiVersion,
	}
}

func GeneratePODObjectMeta(labels map[string]string, annotations map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Labels:      labels,
		Annotations: annotations,
	}
}

func GenerateSTSObjectMeta(name string, namespace string, labels map[string]string, annotations map[string]string, ownerDef []metav1.OwnerReference) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            name,
		Namespace:       namespace,
		Labels:          labels,
		Annotations:     annotations,
		OwnerReferences: ownerDef,
	}
}

func GenerateVolumeFromEmpty(name string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}
}

func GenerateVolumeFromConfigMap(vname string, cmname string) corev1.Volume {
	executeMode := int32(0755)
	return corev1.Volume{
		Name: vname,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: cmname,
				},
				DefaultMode: &executeMode,
			},
		},
	}
}

func GenerateVolumeFromSecret(vname string, srtname string) corev1.Volume {
	return corev1.Volume{
		Name: vname,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: srtname,
			},
		},
	}
}

type DownwardAPIItem struct {
	Filename  string
	Fieldname string
}

func GenerateVolumeFromDownwardAPI(vname string, itemsinfo []DownwardAPIItem) corev1.Volume {
	var items []corev1.DownwardAPIVolumeFile
	for _, v := range itemsinfo {
		items = append(items, corev1.DownwardAPIVolumeFile{
			Path: v.Filename,
			FieldRef: &corev1.ObjectFieldSelector{
				APIVersion: "v1",
				FieldPath:  v.Fieldname,
			},
		})
	}

	var mode int32 = 420
	return corev1.Volume{
		Name: vname,
		VolumeSource: corev1.VolumeSource{
			DownwardAPI: &corev1.DownwardAPIVolumeSource{
				Items:       items,
				DefaultMode: &mode,
			},
		},
	}
}

func GenerateVolumeFromPVC(vname string, pvcname string, readOnly bool) corev1.Volume {
	return corev1.Volume{
		Name: vname,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcname,
				ReadOnly:  readOnly,
			},
		},
	}
}

func GeneratePersistentVolumeClaimTemplates(name string, labels map[string]string, storage v1beta1.RedisPVCStorage, ownerRef []metav1.OwnerReference) corev1.PersistentVolumeClaim {
	mode := corev1.PersistentVolumeFilesystem
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storage.Size,
				},
			},
			StorageClassName: &storage.Class,
			VolumeMode:       &mode,
		},
	}

	if storage.DeleteClaim {
		pvc.OwnerReferences = ownerRef
	}
	return pvc
}

func GenerateContainer(name string, image string, imagePullPolicy corev1.PullPolicy, securityContext *corev1.SecurityContext, ports []corev1.ContainerPort, env []corev1.EnvVar, command []string, args []string, readinessProbe, livenessProbe *corev1.Probe, lifecycle *corev1.Lifecycle, volumeMounts []corev1.VolumeMount, resource corev1.ResourceRequirements) corev1.Container {
	return corev1.Container{
		Name:            name,
		Image:           image,
		ImagePullPolicy: imagePullPolicy,
		SecurityContext: securityContext,
		Ports:           ports,
		Env:             env,
		Command:         command,
		Args:            args,
		ReadinessProbe:  readinessProbe,
		LivenessProbe:   livenessProbe,
		Lifecycle:       lifecycle,
		VolumeMounts:    volumeMounts,
		Resources:       resource,
	}
}

func GeneratePodSpec(containers []corev1.Container, imagePullSecrets []corev1.LocalObjectReference, nodeSelect map[string]string, volumes []corev1.Volume, securityContext *corev1.PodSecurityContext, priorityClassName string, affinity *corev1.Affinity) corev1.PodSpec {
	return corev1.PodSpec{
		ImagePullSecrets:  imagePullSecrets,
		NodeSelector:      nodeSelect,
		SecurityContext:   securityContext,
		PriorityClassName: priorityClassName,
		Affinity:          affinity,
		Containers:        containers,
		Volumes:           volumes,
	}
}

func GenerateStatefulSetSpec(serviceName string, replicas *int32, labels map[string]string, podObjectMeta metav1.ObjectMeta, podSpec corev1.PodSpec, pvcs []corev1.PersistentVolumeClaim) appsv1.StatefulSetSpec {
	return appsv1.StatefulSetSpec{
		ServiceName: serviceName,
		Replicas:    replicas,
		Selector: &metav1.LabelSelector{
			MatchLabels: labels,
		},
		UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
			Type: appsv1.RollingUpdateStatefulSetStrategyType, // TODO: 测试滚动更新和 onDelete 两种情况的效果
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: podObjectMeta,
			Spec:       podSpec,
		},
		VolumeClaimTemplates: pvcs,
	}
}

func GenerateStatefulSets(typeMeta metav1.TypeMeta, objectMeta metav1.ObjectMeta, spec appsv1.StatefulSetSpec) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		TypeMeta:   typeMeta,
		ObjectMeta: objectMeta,
		Spec:       spec,
	}
}
