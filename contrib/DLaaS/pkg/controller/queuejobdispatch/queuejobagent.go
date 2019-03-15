/*
Copyright 2014 The Kubernetes Authors.

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

package queuejobdispatch

import (
	"strings"
	clientset "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/client/clientset/controller-versioned"
	schedulerapi "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/scheduler/api"
)

type XQueueJobAgent struct{
		agentID			string
		deploymentName	string
		clients			*clientset.Clientset
		aggrResouces *schedulerapi.Resource
}

func NewXQueueJobAgent(config string) *XQueueJobAgent {
	configStrings:=strings.Split(config, ";")
	if len(configStrings)<2 {
		return nil
	}
	qa := &XqueueJobAgent{
		agentId:	configStrings[0],
		deplymentName: configStrings[1],
		clients:	clientset.NewForConfigOrDie(config)
	}
	return qa
}

func (qa *XQueueJobAgent) CreateXQueueJob(cqj *arbv1.XQueueJob) error {
	var err error

	return nil
}

func (qa *XQueueJobAgent) UpdateAggrResources() error {
	var err error

	return nil
}
