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
	"github.com/golang/glog"

	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/kubernetes"
	clientset "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/client/clientset/controller-versioned"
	arbv1 "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/apis/controller/v1alpha1"
	schedulerapi "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/scheduler/api"
)

type XQueueJobAgent struct{
		AgentId			string
		DeploymentName	string
		queuejobclients			*clientset.Clientset
		deploymentclients    *kubernetes.Clientset				// for the upate of aggr resouces
		AggrResources *schedulerapi.Resource
}

func NewXQueueJobAgent(config string) *XQueueJobAgent {
	configStrings:=strings.Split(config, ";")
	if len(configStrings)<2 {
		return nil
	}
	agent_config, err:=clientcmd.BuildConfigFromFlags("", "/root/.kube/config_101")
	if err!=nil {
		return nil
	}
	qa := &XQueueJobAgent{
		AgentId:	configStrings[0],
		DeploymentName: configStrings[1],
		queuejobclients:	clientset.NewForConfigOrDie(agent_config),
		deploymentclients:    kubernetes.NewForConfigOrDie(agent_config),
		AggrResources: schedulerapi.EmptyResource(),
	}
	return qa
}

func (qa *XQueueJobAgent) CreateXQueueJob(cqj *arbv1.XQueueJob) {
	glog.Infof("Create XQJ: %s in Agent %s", cqj.Name, qa.AgentId)
	qa.queuejobclients.ArbV1().XQueueJobs(cqj.Namespace).Create(cqj)
	return
}

func (qa *XQueueJobAgent) UpdateAggrResources() error {
	// qa.deploymentclients.AppsV1beta1().Deployments(name).Get(service)
	return nil
}
