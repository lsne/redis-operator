// Created by lsne on 2022-11-06 21:49:02

package k8sutils

func CopyLabels(labels map[string]string) map[string]string {
	l := make(map[string]string)
	for k, v := range labels {
		l[k] = v
	}
	return l
}
