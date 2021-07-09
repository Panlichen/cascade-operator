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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// CascadeConfigMapFinder is where the controller gets layout info for a Cascade instance
type CascadeConfigMapFinder struct {
	// name indicates the name of ConfigMap provided by the user
	Name string `json:"name"`
	// jsonItem indicates the name of the layout json file item inside the ConfigMap
	JsonItem string `json:"jsonItem"`
	// cfgTemplateItem indicates the name of the derecho.cfg.template file item inside the ConfigMap
	CfgTemplateItem string `json:"cfgTemplateItem"`
}

// CascadeSpec defines the desired state of Cascade
type CascadeSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster

	ConfigMapFinder CascadeConfigMapFinder `json:"configMapFinder"`

	// logicalServerSize is the desired logical server number of a Cascade Group
	LogicalServerSize int `json:"logicalServerSize"`

	// clientSize is the desired client number of a Cascade Group
	ClientSize int `json:"clientSize"`
}

// CascadeStatus defines the observed state of Cascade
type CascadeStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Nodes are the names of the cascade pods
	Nodes []string `json:"nodes,omitempty"`

	// physicalServerSize is the physical number of server nodes.
	PhysicalServerSize int `json:"physicalServerSize,omitempty"`
	// logicalServerSize is the logical number of server nodes, which means that overlapped nodes
	// are counted for each appearance.
	LogicalServerSize int `json:"logicalServerSize,omitempty"`

	// clientSize is current number of client nodes.
	ClientSize int `json:"clientSize,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Cascade is the Schema for the cascades API
type Cascade struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CascadeSpec   `json:"spec,omitempty"`
	Status CascadeStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// CascadeList contains a list of Cascade
type CascadeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Cascade `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Cascade{}, &CascadeList{})
}
