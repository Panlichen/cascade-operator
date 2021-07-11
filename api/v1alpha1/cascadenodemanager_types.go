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

package v1alpha1

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (cascadeType *CascadeTypeSpec) String() string {
	jsonObj, _ := json.Marshal(cascadeType)
	return string(jsonObj)
}

// CascadeSubgroupLayoutSpec wraps configuration for each shard in a subgroup
type CascadeSubgroupLayoutSpec struct {
	// min_nodes_by_shard is minNodes for each shard
	MinNodesByShard []int `json:"min_nodes_by_shard"`
	// max_nodes_by_shard is maxNodes for each shard
	MaxNodesByShard []int `json:"max_nodes_by_shard"`
	// reserved_node_id_by_shard is reservedNodeId for each shard, not mandatory
	ReservedNodeIdByShard [][]int `json:"reserved_node_id_by_shard,omitempty"`
	// delivery_modes_by_shard is deliveryMode for each shard
	DeliveryModesByShard []string `json:"delivery_modes_by_shard"`
	// profiles_by_shard is profile for each shard
	ProfilesByShard []string `json:"profiles_by_shard"`
}

// CascadeTypeSpec wraps configuration for each subgroup for a type
type CascadeTypeSpec struct {
	// typeAlias is the name for a type
	TypeAlias       string                      `json:"type_alias"`
	SubgroupsLayout []CascadeSubgroupLayoutSpec `json:"layout"`
}

// CascadeNodeManagerSpec defines the desired state of CascadeNodeManager
type CascadeNodeManagerSpec struct {
	TypesSpec []CascadeTypeSpec `json:"typesSpec"`
}

// CascadeSubgroupStatus wraps status information for shards in a subgroup
type CascadeSubgroupStatus struct {
	// shardCount is the number of shards in this subgroup
	ShardCount int `json:"shardCount,omitempty"`

	// TODO: manage this in ViewManager with View ID?
	// assignedNodeIdByShard is assignedNodeId for each shard, omitted in spec, maintained in status
	AssignedNodeIdByShard [][]int `json:"assignedNodeIdByShard,omitempty"`
	// sizeByShard is the current size of every shard in current subgroup
	SizeByShard []int `json:"sizeByShard,omitempty"`
}

// CascadeTypeStatus maintains Cascade node allocation information during runtime
type CascadeTypeStatus struct {
	// typeAlias is the name for a type
	TypeAlias       string                  `json:"typeAlias"`
	SubgroupsStatus []CascadeSubgroupStatus `json:"subgroupStatus"`
}

// PodMetrics maintains metric collected from MetricServer & Prometheus for a single node.
// CascadeNodeManagerStatus holds a map[string]PodMetrics, the key of which is selected by label related to current Cascade.
type PodMetrics struct {
}

type MachineMetrics struct {
	// machineIP is machineIP
	MachineIP string `json:"machineIP"`

	// memoryTotal is from resource nodes.v1
	MemoryTotal resource.Quantity `json:"memoryTotal"`
	// memoryAvailable is node_memory_MemAvailable_bytes metric from prometheus
	MemoryAvaiable resource.Quantity `json:"memoryAvailable"`
	// memoryUsage is from usage.memory of resource nodes.metrics.k8s.io
	MemoryUsage resource.Quantity `json:"memoryUsage"`

	// cpuTotal is from resource nodes.v1
	CPUTotal resource.Quantity `json:"cpuTotal"`
	// cpuLoad1 is node_load1 metric from prometheus
	CPULoad1 float64 `json:"cpuLoad1"`
	// cpuLoad1 is node_load1 metric from prometheus
	CPULoad5 float64 `json:"cpuLoad5"`
	// cpuLoad1 is node_load1 metric from prometheus
	CPULoad15 float64 `json:"cpuLoad15"`
	// cpuUsage is from usage.cpu of resource nodes.metrics.k8s.io
	CPUUsage resource.Quantity `json:"cpuUsage"`

	// TODO: GPU metrics

	// TODO: Infiniband metrics
}

// CascadeNodeManagerStatus maintains running information.
type CascadeNodeManagerStatus struct {
	TypesStatus []CascadeTypeStatus `json:"typesStatus"`

	// TODO: add some filed to manage node_ids reserved for overlapping after we know clearly how to make use of overlapped shards.

	// maxReservedNodeId is the max reserved node id calculated from configMap
	MaxReservedNodeId int `json:"maxReservedNodeId"`

	// nextNodeIdToAssign is the next non-reserved node id to assign
	NextNodeIdToAssign int `json:"nextNodeIdToAssign"`

	// leastRequiredLogicalNodes is the sum of min_nodes for all shards
	LeastRequiredLogicalNodes int `json:"leastRequiredLogicalNodes"`

	// maxLogicalNodes is the sum of min_nodes for all shards
	MaxLogicalNodes int `json:"maxLogicalNodes"`

	// leaderID is the leader id for this Cascade
	LeaderID int `json:"leaderID"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// CascadeNodeManager stores the node assingment policy defined by user and maintains
// current node status
type CascadeNodeManager struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CascadeNodeManagerSpec   `json:"spec"`
	Status CascadeNodeManagerStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// CascadeNodeManagerList contains a list of CascadeNodeManager
type CascadeNodeManagerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []CascadeNodeManager `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CascadeNodeManager{}, &CascadeNodeManagerList{})
}
