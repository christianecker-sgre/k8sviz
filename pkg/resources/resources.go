// SPDX-FileCopyrightText: 2021 k8sviz authors
// SPDX-License-Identifier: Apache-2.0

package resources

import (
	"context"
	"fmt"
	"strings"

	"github.com/awalterschulze/gographviz"
	appsv1 "k8s.io/api/apps/v1"
	autov1 "k8s.io/api/autoscaling/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var (
	// ResourceTypes represents the set of resource types.
	// Resouces are grouped by the same level of abstraction.
	ResourceTypes   = []string{"hpa cronjob", "deploy job", "sts ds rs", "pod", "pvc", "svc", "ing"}
	normalizedNames = map[string]string{
		"ns":      "namespace",
		"svc":     "service",
		"pvc":     "persistentvolumeclaim",
		"pod":     "po",
		"sts":     "statefulset",
		"ds":      "daemonset",
		"rs":      "replicaset",
		"deploy":  "deployment",
		"job":     "job",
		"cronjob": "cj",
		"ing":     "ingress",
		"hpa":     "horizontalpodautoscaler"}
)

// Resources represents the k8s resources
type Resources struct {
	clientset kubernetes.Interface
	Namespace string

	Svcs      *corev1.ServiceList
	Pvcs      *corev1.PersistentVolumeClaimList
	Pods      *corev1.PodList
	Stss      *appsv1.StatefulSetList
	Dss       *appsv1.DaemonSetList
	Rss       *appsv1.ReplicaSetList
	Deploys   *appsv1.DeploymentList
	Jobs      *batchv1.JobList
	CronJobs  *batchv1.CronJobList
	Ingresses *netv1.IngressList
	Hpas      *autov1.HorizontalPodAutoscalerList
}

type ResourceGraph map[string][]string

// NewResources resturns Resources for the namespace
func NewResources(clientset kubernetes.Interface, namespace string) (*Resources, error) {
	var err error
	res := &Resources{clientset: clientset, Namespace: namespace}

	// service
	res.Svcs, err = clientset.CoreV1().Services(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get services in namespace %q: %v", namespace, err)
	}

	// persistentvolumeclaim
	res.Pvcs, err = clientset.CoreV1().PersistentVolumeClaims(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get persistentVolumeClaims in namespace %q: %v", namespace, err)
	}

	// pod
	res.Pods, err = clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get pods in namespace %q: %v", namespace, err)
	}

	// statefulset
	res.Stss, err = clientset.AppsV1().StatefulSets(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get statefulsets in namespace %q: %v", namespace, err)
	}

	// daemonset
	res.Dss, err = clientset.AppsV1().DaemonSets(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get daemonsets in namespace %q: %v", namespace, err)
	}

	// replicaset
	res.Rss, err = clientset.AppsV1().ReplicaSets(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get replicasets in namespace %q: %v", namespace, err)
	}
	// Remove old rss from the list
	removedList := []appsv1.ReplicaSet{}
	for _, rs := range res.Rss.Items {
		// Old replicaset has both desired replicas and current replicas set to 0
		if rs.Spec.Replicas != nil && *rs.Spec.Replicas == int32(0) && rs.Status.Replicas == int32(0) {
			continue
		}
		removedList = append(removedList, rs)
	}
	res.Rss.Items = removedList

	// deployment
	res.Deploys, err = clientset.AppsV1().Deployments(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get deployments in namespace %q: %v", namespace, err)
	}

	// job
	res.Jobs, err = clientset.BatchV1().Jobs(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get jobs in namespace %q: %v", namespace, err)
	}

	// cronJob
	res.CronJobs, err = clientset.BatchV1().CronJobs(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get cronjobs in namespace %q: %v", namespace, err)
	}

	// ingress
	res.Ingresses, err = clientset.NetworkingV1().Ingresses(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get ingresses in namespace %q: %v", namespace, err)
	}

	// hpas
	res.Hpas, err = clientset.AutoscalingV1().HorizontalPodAutoscalers(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get hpas in namespace %q: %v", namespace, err)
	}

	return res, nil
}

// GetResourceNames returns the resource names of the kind
func (r *Resources) GetResourceNames(kind string) []string {
	names := []string{}

	switch kind {
	case "svc":
		for _, n := range r.Svcs.Items {
			names = append(names, n.Name)
		}
	case "pvc":
		for _, n := range r.Pvcs.Items {
			names = append(names, n.Name)
		}
	case "pod":
		for _, n := range r.Pods.Items {
			names = append(names, n.Name)
		}
	case "sts":
		for _, n := range r.Stss.Items {
			names = append(names, n.Name)
		}
	case "ds":
		for _, n := range r.Dss.Items {
			names = append(names, n.Name)
		}
	case "rs":
		for _, n := range r.Rss.Items {
			names = append(names, n.Name)
		}
	case "deploy":
		for _, n := range r.Deploys.Items {
			names = append(names, n.Name)
		}
	case "job":
		for _, n := range r.Jobs.Items {
			names = append(names, n.Name)
		}
	case "cronjob":
		for _, n := range r.CronJobs.Items {
			names = append(names, n.Name)
		}
	case "ing":
		for _, n := range r.Ingresses.Items {
			names = append(names, n.Name)
		}
	case "hpa":
		for _, n := range r.Hpas.Items {
			names = append(names, n.Name)
		}
	}

	return names
}

// HasResource check if Resources has k8s resource with the kind and the name
func (r *Resources) HasResource(kind, name string) bool {
	for _, resName := range r.GetResourceNames(kind) {
		if resName == name {
			return true
		}
	}
	return false
}

// NormalizeResource resturns normalized name of the resource.
// It returns error if it fails to normalize the resource name.
// key of normalizedNames map is used as the normalized name.
func NormalizeResource(resource string) (string, error) {
	for k, v := range normalizedNames {
		if k == strings.ToLower(resource) {
			return k, nil
		}
		if v == strings.ToLower(resource) {
			return k, nil
		}
	}
	return "", fmt.Errorf("failed to find normalized resource name for %s", resource)
}

func SplitResources(in *Resources, split string, totalGraph *gographviz.Graph) ([]*Resources, error) {
	if split == "" {
		return []*Resources{in}, nil
	}

	// build graph representation
	graph := ResourceGraph{}
	for _, j := range totalGraph.Edges.Edges {
		if _, ok := graph[j.Src]; ok {
			graph[j.Src] = append(graph[j.Src], j.Dst)
		} else {
			graph[j.Src] = []string{j.Dst}
		}
	}

	roots := in.GetResourceNames(split)
	out := make([]*Resources, len(roots))

	svcs := in.Svcs.DeepCopy().Items
	pvcs := in.Pvcs.DeepCopy().Items
	pods := in.Pods.DeepCopy().Items
	stss := in.Stss.DeepCopy().Items
	dss := in.Dss.DeepCopy().Items
	rss := in.Rss.DeepCopy().Items
	deploys := in.Deploys.DeepCopy().Items
	jobs := in.Jobs.DeepCopy().Items
	cronJobs := in.CronJobs.DeepCopy().Items
	ingresses := in.Ingresses.DeepCopy().Items
	hpas := in.Hpas.DeepCopy().Items
	for i, r := range roots {
		root := resourceName(split, r)
		out[i] = &Resources{
			Namespace: in.Namespace,
			Svcs:      &v1.ServiceList{},
			Pvcs:      &v1.PersistentVolumeClaimList{},
			Pods:      &v1.PodList{},
			Stss:      &appsv1.StatefulSetList{},
			Dss:       &appsv1.DaemonSetList{},
			Rss:       &appsv1.ReplicaSetList{},
			Deploys:   &appsv1.DeploymentList{},
			Jobs:      &batchv1.JobList{},
			CronJobs:  &batchv1.CronJobList{},
			Ingresses: &netv1.IngressList{},
			Hpas:      &autov1.HorizontalPodAutoscalerList{},
		}

		out[i].Svcs.Items, svcs = filter(svcs, graph, root)
		out[i].Pvcs.Items, pvcs = filter(pvcs, graph, root)
		out[i].Pods.Items, pods = filter(pods, graph, root)
		out[i].Stss.Items, stss = filter(stss, graph, root)
		out[i].Dss.Items, dss = filter(dss, graph, root)
		out[i].Rss.Items, rss = filter(rss, graph, root)
		out[i].Deploys.Items, deploys = filter(deploys, graph, root)
		out[i].Jobs.Items, jobs = filter(jobs, graph, root)
		out[i].CronJobs.Items, cronJobs = filter(cronJobs, graph, root)
		out[i].Ingresses.Items, ingresses = filter(ingresses, graph, root)
		out[i].Hpas.Items, hpas = filter(hpas, graph, root)
	}

	// add rest
	if len(svcs) > 0 || len(pvcs) > 0 || len(pods) > 0 || len(stss) > 0 || len(dss) > 0 || len(rss) > 0 ||
		len(deploys) > 0 || len(jobs) > 0 || len(cronJobs) > 0 || len(ingresses) > 0 || len(hpas) > 0 {
		out = append(out, &Resources{
			Namespace: in.Namespace,
			Svcs:      &corev1.ServiceList{Items: svcs},
			Pvcs:      &corev1.PersistentVolumeClaimList{Items: pvcs},
			Pods:      &corev1.PodList{Items: pods},
			Stss:      &appsv1.StatefulSetList{Items: stss},
			Dss:       &appsv1.DaemonSetList{Items: dss},
			Rss:       &appsv1.ReplicaSetList{Items: rss},
			Deploys:   &appsv1.DeploymentList{Items: deploys},
			Jobs:      &batchv1.JobList{Items: jobs},
			CronJobs:  &batchv1.CronJobList{Items: cronJobs},
			Ingresses: &netv1.IngressList{Items: ingresses},
			Hpas:      &autov1.HorizontalPodAutoscalerList{Items: hpas},
		})
	}
	return out, nil
}

func filter[T any](residualItems []T, graph ResourceGraph, root string) (inResources []T, otherResources []T) {
	for _, j := range residualItems {
		if isConnected(graph, root, j) {
			inResources = append(inResources, j)
		} else {
			otherResources = append(otherResources, j)
		}
	}

	return inResources, otherResources
}

func isConnected(graph ResourceGraph, root string, targetRes interface{}) bool {
	return DFS(graph, root, getObjectName(targetRes), make(map[string]bool)) || DFS(graph, getObjectName(targetRes), root, make(map[string]bool))
}

func getObjectName(v interface{}) string {
	switch v.(type) {
	case v1.Service:
		return resourceName("svc", v.(v1.Service).Name)
	case v1.PersistentVolumeClaim:
		return resourceName("pvc", v.(v1.PersistentVolumeClaim).Name)
	case v1.Pod:
		return resourceName("pod", v.(v1.Pod).Name)
	case appsv1.StatefulSet:
		return resourceName("sts", v.(appsv1.StatefulSet).Name)
	case appsv1.DaemonSet:
		return resourceName("ds", v.(appsv1.DaemonSet).Name)
	case appsv1.ReplicaSet:
		return resourceName("rs", v.(appsv1.ReplicaSet).Name)
	case appsv1.Deployment:
		return resourceName("deploy", v.(appsv1.Deployment).Name)
	case batchv1.Job:
		return resourceName("job", v.(batchv1.Job).Name)
	case batchv1.CronJob:
		return resourceName("cj", v.(batchv1.CronJob).Name)
	case netv1.Ingress:
		return resourceName("ing", v.(netv1.Ingress).Name)
	case autov1.HorizontalPodAutoscaler:
		return resourceName("hpa", v.(autov1.HorizontalPodAutoscaler).Name)
	default:
		panic("unknown type")
	}
}

func DFS(graph ResourceGraph, start, target string, visited map[string]bool) bool {
	if start == target {
		return true
	}

	visited[start] = true

	for _, neighbor := range graph[start] {
		if !visited[neighbor] {
			if DFS(graph, neighbor, target, visited) {
				return true
			}
		}
	}

	return false
}

func escapeName(name string) string {
	return strings.NewReplacer(".", "_", "-", "_").Replace(name)
}

func resourceName(resType, name string) string {
	return resType + "_" + escapeName(name)
}
