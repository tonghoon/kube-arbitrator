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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type XQueueJobAgent struct{
		AgentId			string
		DeploymentName	string
		queuejobclients			*clientset.Clientset
		deploymentclients    *kubernetes.Clientset				// for the upate of aggr resouces
		AggrResources *schedulerapi.Resource
}

func NewXQueueJobAgent(config string) *XQueueJobAgent {
	configStrings:=strings.Split(config, ":")
	if len(configStrings)<2 {
		return nil
	}
	glog.Infof("[Agnet] Agent %s:%s Created\n", configStrings[0], configStrings[1])

	agent_config, err:=clientcmd.BuildConfigFromFlags("", "/root/agent101config")
	if err!=nil {
		glog.Infof("[Agent] Cannot crate client\n")
		return nil
	}
	qa := &XQueueJobAgent{
		AgentId:	configStrings[0],
		DeploymentName: configStrings[1],
		queuejobclients:	clientset.NewForConfigOrDie(agent_config),
		deploymentclients:    kubernetes.NewForConfigOrDie(agent_config),
		// AggrResources: schedulerapi.EmptyResource(),
	}
	if qa.queuejobclients==nil {
		glog.Infof("[Agnet] Cannot Create Client\n")
	} else {
		glog.Infof("[Agnet] Create Client Suceessfully\n")
	}
	return qa
}

func (qa *XQueueJobAgent) CreateXQueueJob(cqj *arbv1.XQueueJob) {
	glog.Infof("[Agnet] Change XQJ Canrun and ...: %s in Agent %s====================\n", cqj.Name, qa.AgentId)


	qj_temp:=cqj.DeepCopy()
	agent_qj:=arbv1.XQueueJob{
		TypeMeta: qj_tmep.TypeMeta,
		ObjectMeta: metav1.ObjectMeta{Name: qj_temp.Name,},
		Spec: qj_temp.Spec,
	}
	glog.Infof("[Agent] XQJ resourceVersion cleaned")
	// em:=metav1.ObjectMeta{Name: cqj.Name,}
	// em.DeepCopyInto(&copyed_qj.ObjectMeta)
	// copyed_qj.Status.CanRun=false
	// copyed_qj.Status.State=arbv1.QueueJobStateEnqueued

	glog.Infof("Create XQJ: %s in Agent %s====================_with Namespace:%s\n", agent_qj.Name, qa.AgentId)
	qa.queuejobclients.ArbV1().XQueueJobs(agent_qj.Namespace).Create(*agent_qj)

	pods, err := qa.deploymentclients.CoreV1().Pods("").List(metav1.ListOptions{})
	if err != nil {
		glog.Infof("[Agent] Cannot Access Agent================\n")
	}
	glog.Infof("There are %d pods in the cluster\n", len(pods.Items))
	for _, pod := range pods.Items {
		glog.Infof("[Agent] Pod Name=%s\n",pod.Name)
	}


	return
}

func (qa *XQueueJobAgent) UpdateAggrResources() error {
	// qa.deploymentclients.AppsV1beta1().Deployments(name).Get(service)
	return nil
}
