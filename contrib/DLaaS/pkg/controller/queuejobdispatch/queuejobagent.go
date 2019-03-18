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
	"k8s.io/client-go/tools/clientcmd"
	clientset "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/client/clientset/controller-versioned"
	// schedulerapi "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/scheduler/api"
)

type XQueueJobAgent struct{
		AgentId			string
		DeploymentName	string
		clients			*clientset.Clientset
		// aggrResouces *schedulerapi.Resource
}

func NewXQueueJobAgent(config string) *XQueueJobAgent {
	configStrings:=strings.Split(config, ";")
	if len(configStrings)<2 {
		return nil
	}
	agent_config, _:=clientcmd.BuildConfigFromFlags("", configStrings[0])
	qa := &XQueueJobAgent{
		AgentId:	configStrings[0],
		DeploymentName: configStrings[1],
		clients:	clientset.NewForConfigOrDie(agent_config),
	}
	return qa
}

// func (qa *XQueueJobAgent) CreateXQueueJob(cqj *arbv1.XQueueJob) error {
// 	return nil
// }

// func (qa *XQueueJobAgent) UpdateAggrResources() error {
// 	return nil
// }
