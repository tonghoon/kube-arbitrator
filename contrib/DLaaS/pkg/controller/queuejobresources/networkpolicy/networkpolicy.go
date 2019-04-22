/*
Copyright 2017 The Kubernetes Authors.
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

package networkpolicy

// import (
// 	"fmt"
// 	"github.com/golang/glog"
// 	arbv1 "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/apis/controller/v1alpha1"
// 	clientset "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/client/clientset/controller-versioned"
// 	"github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/controller/queuejobresources"
// 	schedulerapi "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/scheduler/api"
// 	"k8s.io/api/core/v1"
// 	"k8s.io/apimachinery/pkg/api/errors"
// 	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
// 	"k8s.io/apimachinery/pkg/runtime"
// 	"k8s.io/apimachinery/pkg/runtime/schema"
// 	"k8s.io/apimachinery/pkg/runtime/serializer/json"
// 	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
// 	"k8s.io/client-go/informers"
// 	"k8s.io/client-go/kubernetes"
// 	"sync"
// 	"time"
//
// 	"k8s.io/apimachinery/pkg/api/meta"
//
// 	ssinformer "k8s.io/client-go/informers/apps/v1"
// 	sslister "k8s.io/client-go/listers/apps/v1"
// 	"k8s.io/api/extensions/v1beta1"
// 	apps "k8s.io/api/apps/v1"
// 	"k8s.io/client-go/rest"
// 	"k8s.io/client-go/tools/cache"
// )


import (
	"fmt"
	"github.com/golang/glog"
	arbv1 "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/apis/controller/v1alpha1"
	clientset "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/client/clientset/controller-versioned"
	"github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/controller/queuejobresources"
	schedulerapi "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/scheduler/api"
	// "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sync"
	"time"


  // testv1	"k8s.io/api/extensions/v1beta1"
	networkingv1 "k8s.io/api/networking/v1"
	networkingv1informer "k8s.io/client-go/informers/networking/v1"
	networkingv1lister "k8s.io/client-go/listers/networking/v1"
)

var queueJobKind = arbv1.SchemeGroupVersion.WithKind("XQueueJob")
var queueJobName = "xqueuejob.arbitrator.k8s.io"

const (
	// QueueJobNameLabel label string for queuejob name
	QueueJobNameLabel string = "xqueuejob-name"

	// ControllerUIDLabel label string for queuejob controller uid
	ControllerUIDLabel string = "controller-uid"
)

//QueueJobResService contains service info
type QueueJobResNetworkPolicy struct {
	clients    *kubernetes.Clientset
	arbclients *clientset.Clientset
	// A store of services, populated by the serviceController
	networkpolicyStore    networkingv1lister.NetworkPolicyLister
	networkpolicyInformer networkingv1informer.NetworkPolicyInformer
	rtScheme        *runtime.Scheme
	jsonSerializer  *json.Serializer
	// Reference manager to manage membership of queuejob resource and its members
	refManager queuejobresources.RefManager
}

//Register registers a queue job resource type
func Register(regs *queuejobresources.RegisteredResources) {
	regs.Register(arbv1.ResourceTypeNetworkPolicy, func(config *rest.Config) queuejobresources.Interface {
		return NewQueueJobResNetworkPolicy(config)
	})
}

//NewQueueJobResService creates a service controller
func NewQueueJobResNetworkPolicy(config *rest.Config) queuejobresources.Interface {
	qjrNetworkPolicy := &QueueJobResNetworkPolicy{
		clients:    kubernetes.NewForConfigOrDie(config),
		arbclients: clientset.NewForConfigOrDie(config),
	}

	qjrNetworkPolicy.networkpolicyInformer = informers.NewSharedInformerFactory(qjrNetworkPolicy.clients, 0).Networking().V1().NetworkPolicies()
	qjrNetworkPolicy.networkpolicyInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch obj.(type) {
				case *networkingv1.NetworkPolicy:
					return true
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    qjrNetworkPolicy.addNetworkPolicy,
				UpdateFunc: qjrNetworkPolicy.updateNetworkPolicy,
				DeleteFunc: qjrNetworkPolicy.deleteNetworkPolicy,
			},
		})

	qjrNetworkPolicy.rtScheme = runtime.NewScheme()
	// v1.AddToScheme(qjrNetworkPolicy.rtScheme)
	networkingv1.AddToScheme(qjrNetworkPolicy.rtScheme) // Tonghoon

	qjrNetworkPolicy.jsonSerializer = json.NewYAMLSerializer(json.DefaultMetaFactory, qjrNetworkPolicy.rtScheme, qjrNetworkPolicy.rtScheme)

	qjrNetworkPolicy.refManager = queuejobresources.NewLabelRefManager()

	return qjrNetworkPolicy
}

// Run the main goroutine responsible for watching and services.
func (qjrNetworkPolicy *QueueJobResNetworkPolicy) Run(stopCh <-chan struct{}) {

	qjrNetworkPolicy.networkpolicyInformer.Informer().Run(stopCh)
}

func (qjrPod *QueueJobResNetworkPolicy) GetAggregatedResources(job *arbv1.XQueueJob) *schedulerapi.Resource {
	return schedulerapi.EmptyResource()
}

func (qjrNetworkPolicy *QueueJobResNetworkPolicy) addNetworkPolicy(obj interface{}) {

	return
}

func (qjrNetworkPolicy *QueueJobResNetworkPolicy) updateNetworkPolicy(old, cur interface{}) {

	return
}

func (qjrNetworkPolicy *QueueJobResNetworkPolicy) deleteNetworkPolicy(obj interface{}) {

	return
}


func (qjrPod *QueueJobResNetworkPolicy) GetAggregatedResourcesByPriority(priority int, job *arbv1.XQueueJob) *schedulerapi.Resource {
        total := schedulerapi.EmptyResource()
        return total
}


// Parse queue job api object to get Service template
func (qjrNetworkPolicy *QueueJobResNetworkPolicy) getNetworkPolicyTemplate(qjobRes *arbv1.XQueueJobResource) (*networkingv1.NetworkPolicy, error) {

	// networkpolicyGVK := schema.GroupVersion{Group: networkingv1.GroupName, Version: "v1"}
	// networkpolicyGVK := schema.GroupVersion{Group: networkingv1.GroupName, Version: "v1"}.WithKind("NetworkPolicy")
	// networkpolicyGVK := schema.GroupVersionKind{Group: "extensions", Version: "v1beta1"}.WithKind("NetworkPolicy")

	// networkpolicyGVK := schema.GroupVersion{Group: , Version: "v1"}
	networkpolicyGVK := schema.GroupVersion{Group: networkingv1.GroupName, Version: "v1"}.WithKind("NetworkPolicy")

	glog.Infof("GroupName: %s\n", networkpolicyGVK.Group)
	glog.Infof("Version: %s\n", networkpolicyGVK.Version)
	glog.Infof("Kind: %s\n", networkpolicyGVK.Kind)

	obj, _, err := qjrNetworkPolicy.jsonSerializer.Decode(qjobRes.Template.Raw, &networkpolicyGVK, nil)
	if err != nil {
		glog.Infof("Decoding Error for NetworkPolicy=================================================")
		return nil, err
	}

	networkpolicy, ok := obj.(*networkingv1.NetworkPolicy)
	if !ok {
		return nil, fmt.Errorf("Queuejob resource not defined as a NetworkPolicy")
	}

	return networkpolicy, nil

}

func (qjrNetworkPolicy *QueueJobResNetworkPolicy) createNetworkPolicyWithControllerRef(namespace string, networkpolicy *networkingv1.NetworkPolicy, controllerRef *metav1.OwnerReference) error {

	glog.V(4).Infof("==========create NetworkPolicy: %+v \n", networkpolicy)
	if controllerRef != nil {
		networkpolicy.OwnerReferences = append(networkpolicy.OwnerReferences, *controllerRef)
	}

	if _, err := qjrNetworkPolicy.clients.Networking().NetworkPolicies(namespace).Create(networkpolicy); err != nil {
		return err
	}

	return nil
}

func (qjrNetworkPolicy *QueueJobResNetworkPolicy) delNetworkPolicy(namespace string, name string) error {

	glog.V(4).Infof("==========delete networkpolicy: %s \n", name)
	if err := qjrNetworkPolicy.clients.Networking().NetworkPolicies(namespace).Delete(name, nil); err != nil {
		return err
	}

	return nil
}

func (qjrPod *QueueJobResNetworkPolicy) UpdateQueueJobStatus(queuejob *arbv1.XQueueJob) error {
	return nil
}

//SyncQueueJob syncs the services
func (qjrNetworkPolicy *QueueJobResNetworkPolicy) SyncQueueJob(queuejob *arbv1.XQueueJob, qjobRes *arbv1.XQueueJobResource) error {

	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing queue job resource %q (%v)", qjobRes.Template, time.Now().Sub(startTime))
	}()

	networkpolicies, err := qjrNetworkPolicy.getNetworkPolicyForQueueJobRes(qjobRes, queuejob)
	if err != nil {
		return err
	}

	networkpolicyLen := len(networkpolicies)

	replicas := qjobRes.Replicas

	diff := int(replicas) - int(networkpolicyLen)

	glog.V(4).Infof("QJob: %s had %d networkpolicies and %d desired networkpolicies", queuejob.Name, replicas, networkpolicyLen)

	if diff > 0 {
		template, err := qjrNetworkPolicy.getNetworkPolicyTemplate(qjobRes)
		if err != nil {
			glog.Errorf("Cannot read template from resource %+v %+v", qjobRes, err)
			return err
		}
		//TODO: need set reference after Service has been really added
		tmpNetworkPolicy := networkingv1.NetworkPolicy{}
		err = qjrNetworkPolicy.refManager.AddReference(qjobRes, &tmpNetworkPolicy)
		if err != nil {
			glog.Errorf("Cannot add reference to networkpolicy resource %+v", err)
			return err
		}

		if template.Labels == nil {
			template.Labels = map[string]string{}
		}
		for k, v := range tmpNetworkPolicy.Labels {
			template.Labels[k] = v
		}
		wait := sync.WaitGroup{}
		wait.Add(int(diff))
		for i := 0; i < diff; i++ {
			go func() {
				defer wait.Done()
				err := qjrNetworkPolicy.createNetworkPolicyWithControllerRef(queuejob.Namespace, template, metav1.NewControllerRef(queuejob, queueJobKind))
				if err != nil && errors.IsTimeout(err) {
					return
				}
				if err != nil {
					defer utilruntime.HandleError(err)
				}
			}()
		}
		wait.Wait()
	}

	return nil
}

func (qjrNetworkPolicy *QueueJobResNetworkPolicy) getNetworkPolicyForQueueJob(j *arbv1.XQueueJob) ([]*networkingv1.NetworkPolicy, error) {
	networkpolicylist, err := qjrNetworkPolicy.clients.NetworkingV1().NetworkPolicies(j.Namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	networkpolicies := []*networkingv1.NetworkPolicy{}
	for i, networkpolicy := range networkpolicylist.Items {
		metaNetworkPolicy, err := meta.Accessor(&networkpolicy)
		if err != nil {
			return nil, err
		}

		controllerRef := metav1.GetControllerOf(metaNetworkPolicy)
		if controllerRef != nil {
			if controllerRef.UID == j.UID {
				networkpolicies = append(networkpolicies, &networkpolicylist.Items[i])
			}
		}
	}
	return networkpolicies, nil

}

func (qjrNetworkPolicy *QueueJobResNetworkPolicy) getNetworkPolicyForQueueJobRes(qjobRes *arbv1.XQueueJobResource, j *arbv1.XQueueJob) ([]*networkingv1.NetworkPolicy, error) {

	networkpolicies, err := qjrNetworkPolicy.getNetworkPolicyForQueueJob(j)
	if err != nil {
		return nil, err
	}

	myNetworkPolicies := []*networkingv1.NetworkPolicy{}
	for i, networkpolicy := range networkpolicies {
		if qjrNetworkPolicy.refManager.BelongTo(qjobRes, networkpolicy) {
			myNetworkPolicies = append(myNetworkPolicies, networkpolicies[i])
		}
	}

	return myNetworkPolicies, nil

}

func (qjrNetworkPolicy *QueueJobResNetworkPolicy) deleteQueueJobResNetworkPolicies(qjobRes *arbv1.XQueueJobResource, queuejob *arbv1.XQueueJob) error {

	job := *queuejob

	activeNetworkPolicies, err := qjrNetworkPolicy.getNetworkPolicyForQueueJobRes(qjobRes, queuejob)
	if err != nil {
		return err
	}

	active := int32(len(activeNetworkPolicies))

	wait := sync.WaitGroup{}
	wait.Add(int(active))
	for i := int32(0); i < active; i++ {
		go func(ix int32) {
			defer wait.Done()
			if err := qjrNetworkPolicy.delNetworkPolicy(queuejob.Namespace, activeNetworkPolicies[ix].Name); err != nil {
				defer utilruntime.HandleError(err)
				glog.V(2).Infof("Failed to delete %v, queue job %q/%q deadline exceeded", activeNetworkPolicies[ix].Name, job.Namespace, job.Name)
			}
		}(i)
	}
	wait.Wait()

	return nil
}

//Cleanup deletes all services
func (qjrNetworkPolicy *QueueJobResNetworkPolicy) Cleanup(queuejob *arbv1.XQueueJob, qjobRes *arbv1.XQueueJobResource) error {
	return qjrNetworkPolicy.deleteQueueJobResNetworkPolicies(qjobRes, queuejob)
}
