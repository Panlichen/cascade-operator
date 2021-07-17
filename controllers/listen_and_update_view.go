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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	derechov1alpha1 "github.com/Panlichen/cascade-operator/api/v1alpha1"
)

func (r *CascadeReconciler) listenUpdateView() {
	ctx := context.Background()
	log := ctrllog.FromContext(ctx)

	viewHandler := func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Error(err, "Fail to read body of view request")
		}
		log.Info(fmt.Sprintf("The request body is %+v", string(body)))
		tempCascadeNMStatus := &derechov1alpha1.CascadeNodeManagerStatus{}
		json.Unmarshal(body, tempCascadeNMStatus)

		cascadeName := strings.Split(tempCascadeNMStatus.LeaderIP, ".")[1]

		r.NodeManagerMap[cascadeName].Status.ViewID = tempCascadeNMStatus.ViewID
		r.NodeManagerMap[cascadeName].Status.LeaderIP = tempCascadeNMStatus.LeaderIP
		for typeID, typeStatus := range tempCascadeNMStatus.TypesStatus {
			for subgroupID, subgroupStatus := range typeStatus.SubgroupsStatus {
				r.NodeManagerMap[cascadeName].Status.TypesStatus[typeID].SubgroupsStatus[subgroupID].AssignedNodeIdByShard = subgroupStatus.AssignedNodeIdByShard
				for shardID, nodesInShard := range subgroupStatus.AssignedNodeIdByShard {
					r.NodeManagerMap[cascadeName].Status.TypesStatus[typeID].SubgroupsStatus[subgroupID].SizeByShard[shardID] = len(nodesInShard)
				}
			}
		}
		log.Info(fmt.Sprintf("After process request from the leader of Cascade %v, it's Status is now %+v", cascadeName, r.NodeManagerMap[cascadeName].Status))
		// TODO: Cannot update ViewID in the CR, don't know why. Maybe we have too many fields in CascadeNodeManagerStatus? Not very important now.
		err = r.Status().Update(ctx, r.NodeManagerMap[cascadeName])
		if err != nil {
			log.Error(err, fmt.Sprintf("Fail to update CascadeNodeManager %v's status in listenUpdateView", cascadeName))
		}

		tempCascadeNodeManager := &derechov1alpha1.CascadeNodeManager{}
		r.Get(ctx, types.NamespacedName{Name: cascadeName, Namespace: "default"}, tempCascadeNodeManager)
		log.Info(fmt.Sprintf("the existing CascadeNodeManager resource is %+v", tempCascadeNodeManager))
	}

	http.HandleFunc("/", viewHandler)
	http.ListenAndServe(":3680", nil)
}
