/*
Copyright 2021.

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

package controllers

import (
	"context"
	goerrors "errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/tidwall/gjson"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	derechov1alpha1 "github.com/Panlichen/cascade-operator/api/v1alpha1"
)

const (
	// Prometheus fqdn:
	promFQDN = "http://monitor-kube-prometheus-st-prometheus.default.svc.cluster.local:9090/api/v1/query?query="

	// Prometheus metrics string constants.
	// Doc: https://github.com/google/cadvisor/blob/master/docs/storage/prometheus.md
	// metrics about machines:
	memoryAvailable = "node_memory_MemAvailable_bytes"
	cpuLoad1        = "node_load1"
	cpuLoad5        = "node_load5"
	cpuLoad15       = "node_load15"

	scheduleInterval = 5 * time.Second

// metrics about pods:
)

func getJsonReplyFromProm(metricName string, log logr.Logger) (string, error) {
	url := promFQDN + metricName
	resp, err := http.Get(url)
	if err != nil {
		log.Error(err, fmt.Sprintf("Fail to get metric %v from prometheus server", metricName))
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err, fmt.Sprintf("Fail to read body for metric %v from prometheus server", metricName))
		return "", err
	}
	return string(body), nil

}

func (r *CascadeReconciler) getMahcinesMetrics(ctx context.Context, log logr.Logger) {
	machineList := &v1.NodeList{}
	r.List(ctx, machineList)

	// collect prometheus metrics for all machines.
	// I think retrieve all metric from the server with one http request and then parse data locally is faster then issue many http requests.
	memoryAvailableJsonStr, _ := getJsonReplyFromProm(memoryAvailable, log)
	cpuLoad1JsonStr, _ := getJsonReplyFromProm(cpuLoad1, log)
	cpuLoad5JsonStr, _ := getJsonReplyFromProm(cpuLoad5, log)
	cpuLoad15JsonStr, _ := getJsonReplyFromProm(cpuLoad15, log)

	for _, machine := range machineList.Items {
		if _, ok := r.MachinesMetrics[machine.Name]; !ok {
			r.MachinesMetrics[machine.Name] = &derechov1alpha1.MachineMetrics{}

			for _, machineAddress := range machine.Status.Addresses {
				if machineAddress.Type == v1.NodeInternalIP {
					r.MachinesMetrics[machine.Name].MachineIP = machineAddress.Address
				}
			}

			r.MachinesMetrics[machine.Name].CPUTotal = machine.Status.Capacity.Cpu().DeepCopy()
			(&r.MachinesMetrics[machine.Name].CPUTotal).SetMilli((&r.MachinesMetrics[machine.Name].CPUTotal).Value())
			r.MachinesMetrics[machine.Name].MemoryTotal = machine.Status.Capacity.Memory().DeepCopy()
			(&r.MachinesMetrics[machine.Name].MemoryTotal).SetMilli((&r.MachinesMetrics[machine.Name].MemoryTotal).Value())
		}
		// query metrics from metrics server(MS)
		machineMetricFromMS := &metricsv1beta1.NodeMetrics{}
		r.Get(ctx, types.NamespacedName{Name: machine.Name, Namespace: ""}, machineMetricFromMS)

		r.MachinesMetrics[machine.Name].MemoryUsage = machineMetricFromMS.Usage.Memory().DeepCopy()
		(&r.MachinesMetrics[machine.Name].MemoryUsage).SetMilli((&r.MachinesMetrics[machine.Name].MemoryUsage).Value())
		r.MachinesMetrics[machine.Name].MemoryUsagePercentage = float64((&r.MachinesMetrics[machine.Name].MemoryUsage).Value()) / float64((&r.MachinesMetrics[machine.Name].MemoryTotal).Value())

		r.MachinesMetrics[machine.Name].CPUUsage = machineMetricFromMS.Usage.Cpu().DeepCopy()
		(&r.MachinesMetrics[machine.Name].CPUUsage).SetMilli((&r.MachinesMetrics[machine.Name].CPUUsage).Value())
		r.MachinesMetrics[machine.Name].CPUUsagePercentage = float64((&r.MachinesMetrics[machine.Name].CPUUsage).Value()) / float64((&r.MachinesMetrics[machine.Name].CPUTotal).Value())

		// parse metrics from prometheus
		// for coding convenience, memoryAvailableJsonStr and other metrics are kind of “asymmetrical”
		metricsLen := gjson.Get(memoryAvailableJsonStr, "data.result.#").Int()
		var i int64
		for i = 0; i < metricsLen; i++ {
			machineIPPath := fmt.Sprintf("data.result.%v.metric.instance", i)
			machineIP := gjson.Get(memoryAvailableJsonStr, machineIPPath).String()
			machineIP = machineIP[:strings.Index(machineIP, ":")]

			if machineIP == r.MachinesMetrics[machine.Name].MachineIP {
				valuePath := fmt.Sprintf("data.result.%v.value", i)

				memoryAvailableValueArray := gjson.Get(memoryAvailableJsonStr, valuePath).Array()
				r.MachinesMetrics[machine.Name].MemoryAvaiable = resource.MustParse(memoryAvailableValueArray[1].String())
				(&r.MachinesMetrics[machine.Name].MemoryAvaiable).SetMilli((&r.MachinesMetrics[machine.Name].MemoryAvaiable).Value())
				r.MachinesMetrics[machine.Name].MemoryAvaiablePercentage = float64((&r.MachinesMetrics[machine.Name].MemoryAvaiable).Value()) / float64((&r.MachinesMetrics[machine.Name].MemoryTotal).Value())

				cpuLoad1ValueArray := gjson.Get(cpuLoad1JsonStr, valuePath).Array()
				r.MachinesMetrics[machine.Name].CPULoad1 = cpuLoad1ValueArray[1].Float()

				cpuLoad5ValueArray := gjson.Get(cpuLoad5JsonStr, valuePath).Array()
				r.MachinesMetrics[machine.Name].CPULoad5 = cpuLoad5ValueArray[1].Float()

				cpuLoad15ValueArray := gjson.Get(cpuLoad15JsonStr, valuePath).Array()
				r.MachinesMetrics[machine.Name].CPULoad15 = cpuLoad15ValueArray[1].Float()
				break
			}
		}
	}
}

// TODO: handel errors, just look better
func (r *CascadeReconciler) getPodMetrics(ctx context.Context, log logr.Logger) {
	// List all cascade pods
	podList := &v1.PodList{}
	appLabel := make(map[string]string)
	appLabel[appKey] = appValue
	listOpts := []client.ListOption{
		client.MatchingLabels(appLabel),
	}
	if err := r.List(ctx, podList, listOpts...); err != nil {
		log.Error(err, "[getPodMetrics] Failed to list cascade pods")
		// TODO: return err
	}
	for _, pod := range podList.Items {
		cascadeInstance, ok := pod.Labels[selectorKey]
		if !ok {
			log.Error(goerrors.New("pod does not has label"), fmt.Sprintf("Pod %v does not has label %v.", pod.Name, selectorKey))
		}
		cascadeNodeManager, ok := r.NodeManagerMap[cascadeInstance]
		if !ok {
			log.Error(goerrors.New("does not has cascade"), fmt.Sprintf("Do not know a cascade %v.", cascadeInstance))
		}
		podMetrics, ok := cascadeNodeManager.Status.PodsMetrics[pod.Name]
		if !ok {
			// current cascade collect current pod's info for the first time
			cascadeNodeManager.Status.PodsMetrics[pod.Name] = &derechov1alpha1.PodMetrics{}
			podMetrics = cascadeNodeManager.Status.PodsMetrics[pod.Name]

			for _, container := range pod.Spec.Containers {
				(&podMetrics.CPURequest).Add(container.Resources.Requests.Cpu().DeepCopy())
				(&podMetrics.CPURequest).SetMilli((&podMetrics.CPURequest).Value())

				(&podMetrics.MemoryRequest).Add(container.Resources.Requests.Memory().DeepCopy())
				(&podMetrics.MemoryRequest).SetMilli((&podMetrics.MemoryRequest).Value())

				(&podMetrics.CPULimit).Add(container.Resources.Limits.Cpu().DeepCopy())
				(&podMetrics.CPULimit).SetMilli((&podMetrics.CPULimit).Value())
				(&podMetrics.MemoryLimit).Add(container.Resources.Limits.Memory().DeepCopy())
				(&podMetrics.MemoryLimit).SetMilli((&podMetrics.MemoryLimit).Value())
			}
		}
		podMetricFromMS := &metricsv1beta1.PodMetrics{}
		r.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, podMetricFromMS)
		for _, containerMetric := range podMetricFromMS.Containers {
			(&podMetrics.CPUUsage).Add(containerMetric.Usage.Cpu().DeepCopy())
			(&podMetrics.CPUUsage).SetMilli((&podMetrics.CPUUsage).Value())

			(&podMetrics.MemoryUsage).Add(containerMetric.Usage.Memory().DeepCopy())
			(&podMetrics.MemoryUsage).SetMilli((&podMetrics.MemoryUsage).Value())

			// cpuUsagePercentage := float32((&podMetrics.CPUUsage).Value()) / float32((&podMetrics.CPULimit).Value())
			// pmemoryUsagePercentage := float32((&podMetrics.MemoryUsage).Value()) / float32((&podMetrics.MemoryLimit).Value())
		}
	}
}

// Run forever, quits when main() quits.
// TODO: handel errors, just look better
func (r *CascadeReconciler) observeAndSchedule() {
	for {
		ctx := context.Background()
		log := ctrllog.FromContext(ctx)
		time.Sleep(5 * time.Second)
		r.getMahcinesMetrics(ctx, log)
		for name, value := range r.MachinesMetrics {
			// if we really want to print resource.Quantity value, we need to transfer it with Value() API.
			log.Info(fmt.Sprintf("The Machines %v Metrics: %+v", name, *value))
		}
		r.getPodMetrics(ctx, log)
		for cascadeName, nodeManager := range r.NodeManagerMap {
			for podName, podMetrics := range nodeManager.Status.PodsMetrics {
				log.Info(fmt.Sprintf("Pod %v of Cascade %v has metrics %+v", podName, cascadeName, podMetrics))
			}
		}

		time.Sleep(scheduleInterval)
	}
}
