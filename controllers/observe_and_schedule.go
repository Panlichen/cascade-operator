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

	// TODO: refine the format of metrics with resource.Quantity type
	for _, machine := range machineList.Items {
		if _, ok := r.MachinesMetrics[machine.Name]; !ok {
			r.MachinesMetrics[machine.Name] = &derechov1alpha1.MachineMetrics{}

			for _, machineAddress := range machine.Status.Addresses {
				if machineAddress.Type == v1.NodeInternalIP {
					r.MachinesMetrics[machine.Name].MachineIP = machineAddress.Address
				}
			}

			r.MachinesMetrics[machine.Name].CPUTotal = machine.Status.Capacity.Cpu().DeepCopy()
			r.MachinesMetrics[machine.Name].MemoryTotal = machine.Status.Capacity.Memory().DeepCopy()
		}
		// query metrics from metrics server(MS)
		machineMetricFromMS := &metricsv1beta1.NodeMetrics{}
		r.Get(ctx, types.NamespacedName{Name: machine.Name, Namespace: ""}, machineMetricFromMS)
		r.MachinesMetrics[machine.Name].CPUUsage = machineMetricFromMS.Usage.Cpu().DeepCopy()
		r.MachinesMetrics[machine.Name].MemoryUsage = machineMetricFromMS.Usage.Memory().DeepCopy()

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

// Run forever, quits when main() quits.
func (r *CascadeReconciler) observeAndSchedule() {
	for {
		ctx := context.Background()
		log := ctrllog.FromContext(ctx)
		time.Sleep(5 * time.Second)
		r.getMahcinesMetrics(ctx, log)
		for name, value := range r.MachinesMetrics {
			log.Info(fmt.Sprintf("The Machines %v Metrics: %+v", name, *value))
		}
	}
}
