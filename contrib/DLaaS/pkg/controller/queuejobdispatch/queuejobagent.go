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
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

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
		// DeploymentName: configStrings[1],
		DeploymentName: "voting-moose-kube-arbitrator",
		queuejobclients:	clientset.NewForConfigOrDie(agent_config),
		deploymentclients:    kubernetes.NewForConfigOrDie(agent_config),
		AggrResources: schedulerapi.EmptyResource(),
	}
	if qa.queuejobclients==nil {
		glog.Infof("[Agnet] Cannot Create Client\n")
	} else {
		glog.Infof("[Agnet] Create Client Suceessfully\n")
	}
	qa.UpdateAggrResources()
	return qa
}

func (qa *XQueueJobAgent) DeleteXQueueJob(cqj *arbv1.XQueueJob) {
	qj_temp:=cqj.DeepCopy()
	glog.Infof("[Agent] XQueueJob is deleted from Agent %s\n", qj_temp.Name, qa.AgentId)
	qa.queuejobclients.ArbV1().XQueueJobs(qj_temp.Namespace).Delete(qj_temp.Name,  &metav1.DeleteOptions{})
	return
}

func (qa *XQueueJobAgent) CreateXQueueJob(cqj *arbv1.XQueueJob) {
	glog.Infof("[Agnet] Change XQJ Canrun and ...: %s in Agent %s====================\n", cqj.Name, qa.AgentId)

	qj_temp:=cqj.DeepCopy()
	agent_qj:=&arbv1.XQueueJob{
		TypeMeta: qj_temp.TypeMeta,
		ObjectMeta: metav1.ObjectMeta{Name: qj_temp.Name, Namespace: qj_temp.Namespace,},
		Spec: qj_temp.Spec,
	}
	glog.Infof("[Agent] XQJ resourceVersion cleaned--Name:%s, Kind:%s\n", agent_qj.Name, agent_qj.Kind)
	glog.Infof("Create XQJ: %s in Agent %s====================\n", agent_qj.Name, qa.AgentId)
	qa.queuejobclients.ArbV1().XQueueJobs(agent_qj.Namespace).Create(agent_qj)

	// pods, err := qa.deploymentclients.CoreV1().Pods("").List(metav1.ListOptions{})
	// if err != nil {
	// 	glog.Infof("[Agent] Cannot Access Agent================\n")
	// }
	// glog.Infof("There are %d pods in the cluster\n", len(pods.Items))
	// // for _, pod := range pods.Items {
	// 	glog.Infof("[Agent] Pod Name=%s\n",pod.Name)
	// }

	return
}

func (qa *XQueueJobAgent) UpdateAggrResources() error {
    glog.Infof("[Agent] Getting aggregated resources for Agent ID: %s with Agent QueueJob Name: %s\n", qa.AgentId, qa.DeploymentName)

    // Read the Agent XQJ Deployment object
    agentXQJDeployment, getErr := qa.deploymentclients.AppsV1().Deployments("kube-system").Get(qa.DeploymentName, metav1.GetOptions{})
    if getErr != nil {
        glog.Infof("Failed to get deployment Agent ID: %s with Agent QueueJob Name: %s, Error: %v\n", qa.AgentId, qa.DeploymentName, getErr)
    }
    for key, value := range agentXQJDeployment.Labels {
        glog.Infof("Agent QueueJob Name: %s has %s=%s: %s\n", qa.DeploymentName, key, value)
    }
    // qa.AggrResources=buildResource(agentXQJDeployment.Labels["available_cpu"], agentXQJDeployment.Labels["available_memory"])
		qa.AggrResources=buildResource("999999","99999999")
    return nil
}

func buildResource(cpu string, memory string) *schedulerapi.Resource {
    return schedulerapi.NewResource(v1.ResourceList{
        v1.ResourceCPU:    resource.MustParse(cpu),
        v1.ResourceMemory: resource.MustParse(memory),
    })
}
