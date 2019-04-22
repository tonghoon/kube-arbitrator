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

package configmap

import (
	"fmt"
	"github.com/golang/glog"
	arbv1 "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/apis/controller/v1alpha1"
	clientset "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/client/clientset/controller-versioned"
	"github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/controller/queuejobresources"
	schedulerapi "github.com/kubernetes-sigs/kube-batch/contrib/DLaaS/pkg/scheduler/api"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	corev1informer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sync"
	"time"
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
type QueueJobResConfigMap struct {
	clients    *kubernetes.Clientset
	arbclients *clientset.Clientset
	// A store of services, populated by the serviceController
	configmapStore    corelisters.ConfigMapLister
	configmapInformer corev1informer.ConfigMapInformer
	rtScheme        *runtime.Scheme
	jsonSerializer  *json.Serializer
	// Reference manager to manage membership of queuejob resource and its members
	refManager queuejobresources.RefManager
}

//Register registers a queue job resource type
func Register(regs *queuejobresources.RegisteredResources) {
	regs.Register(arbv1.ResourceTypeConfigMap, func(config *rest.Config) queuejobresources.Interface {
		return NewQueueJobResConfigMap(config)
	})
}

//NewQueueJobResService creates a service controller
func NewQueueJobResConfigMap(config *rest.Config) queuejobresources.Interface {
	qjrConfigMap := &QueueJobResConfigMap{
		clients:    kubernetes.NewForConfigOrDie(config),
		arbclients: clientset.NewForConfigOrDie(config),
	}

	qjrConfigMap.configmapInformer = informers.NewSharedInformerFactory(qjrConfigMap.clients, 0).Core().V1().ConfigMaps()
	qjrConfigMap.configmapInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch obj.(type) {
				case *v1.ConfigMap:
					return true
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    qjrConfigMap.addConfigMap,
				UpdateFunc: qjrConfigMap.updateConfigMap,
				DeleteFunc: qjrConfigMap.deleteConfigMap,
			},
		})

	qjrConfigMap.rtScheme = runtime.NewScheme()
	v1.AddToScheme(qjrConfigMap.rtScheme)

	qjrConfigMap.jsonSerializer = json.NewYAMLSerializer(json.DefaultMetaFactory, qjrConfigMap.rtScheme, qjrConfigMap.rtScheme)

	qjrConfigMap.refManager = queuejobresources.NewLabelRefManager()

	return qjrConfigMap
}

// Run the main goroutine responsible for watching and services.
func (qjrConfigMap *QueueJobResConfigMap) Run(stopCh <-chan struct{}) {

	qjrConfigMap.configmapInformer.Informer().Run(stopCh)
}

func (qjrPod *QueueJobResConfigMap) GetAggregatedResources(job *arbv1.XQueueJob) *schedulerapi.Resource {
	return schedulerapi.EmptyResource()
}

func (qjrConfigMap *QueueJobResConfigMap) addConfigMap(obj interface{}) {

	return
}

func (qjrConfigMap *QueueJobResConfigMap) updateConfigMap(old, cur interface{}) {

	return
}

func (qjrConfigMap *QueueJobResConfigMap) deleteConfigMap(obj interface{}) {

	return
}


func (qjrPod *QueueJobResConfigMap) GetAggregatedResourcesByPriority(priority int, job *arbv1.XQueueJob) *schedulerapi.Resource {
        total := schedulerapi.EmptyResource()
        return total
}


// Parse queue job api object to get Service template
func (qjrConfigMap *QueueJobResConfigMap) getConfigMapTemplate(qjobRes *arbv1.XQueueJobResource) (*v1.ConfigMap, error) {

	configmapGVK := schema.GroupVersion{Group: v1.GroupName, Version: "v1"}.WithKind("ConfigMap")

	obj, _, err := qjrConfigMap.jsonSerializer.Decode(qjobRes.Template.Raw, &configmapGVK, nil)
	if err != nil {
		return nil, err
	}

	configmap, ok := obj.(*v1.ConfigMap)
	if !ok {
		return nil, fmt.Errorf("Queuejob resource not defined as a ConfigMap")
	}

	return configmap, nil

}

func (qjrConfigMap *QueueJobResConfigMap) createConfigMapWithControllerRef(namespace string, configmap *v1.ConfigMap, controllerRef *metav1.OwnerReference) error {

	glog.V(4).Infof("==========create ConfigMap: %+v \n", configmap)
	if controllerRef != nil {
		configmap.OwnerReferences = append(configmap.OwnerReferences, *controllerRef)
	}

	if _, err := qjrConfigMap.clients.Core().ConfigMaps(namespace).Create(configmap); err != nil {
		return err
	}

	return nil
}

func (qjrConfigMap *QueueJobResConfigMap) delConfigMap(namespace string, name string) error {

	glog.V(4).Infof("==========delete configmap: %s \n", name)
	if err := qjrConfigMap.clients.Core().ConfigMaps(namespace).Delete(name, nil); err != nil {
		return err
	}

	return nil
}

func (qjrPod *QueueJobResConfigMap) UpdateQueueJobStatus(queuejob *arbv1.XQueueJob) error {
	return nil
}

//SyncQueueJob syncs the services
func (qjrConfigMap *QueueJobResConfigMap) SyncQueueJob(queuejob *arbv1.XQueueJob, qjobRes *arbv1.XQueueJobResource) error {

	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing queue job resource %q (%v)", qjobRes.Template, time.Now().Sub(startTime))
	}()

	configmaps, err := qjrConfigMap.getConfigMapForQueueJobRes(qjobRes, queuejob)
	if err != nil {
		return err
	}

	configmapLen := len(configmaps)
	replicas := qjobRes.Replicas

	diff := int(replicas) - int(configmapLen)

	glog.V(4).Infof("QJob: %s had %d configmaps and %d desired configmaps", queuejob.Name, replicas, configmapLen)

	if diff > 0 {
		template, err := qjrConfigMap.getConfigMapTemplate(qjobRes)
		if err != nil {
			glog.Errorf("Cannot read template from resource %+v %+v", qjobRes, err)
			return err
		}
		//TODO: need set reference after Service has been really added
		tmpConfigMap := v1.ConfigMap{}
		err = qjrConfigMap.refManager.AddReference(qjobRes, &tmpConfigMap)
		if err != nil {
			glog.Errorf("Cannot add reference to configmap resource %+v", err)
			return err
		}

		if template.Labels == nil {
			template.Labels = map[string]string{}
		}
		for k, v := range tmpConfigMap.Labels {
			template.Labels[k] = v
		}
		wait := sync.WaitGroup{}
		wait.Add(int(diff))
		for i := 0; i < diff; i++ {
			go func() {
				defer wait.Done()
				err := qjrConfigMap.createConfigMapWithControllerRef(queuejob.Namespace, template, metav1.NewControllerRef(queuejob, queueJobKind))
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

func (qjrConfigMap *QueueJobResConfigMap) getConfigMapForQueueJob(j *arbv1.XQueueJob) ([]*v1.ConfigMap, error) {
	configmaplist, err := qjrConfigMap.clients.CoreV1().ConfigMaps(j.Namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	configmaps := []*v1.ConfigMap{}
	for i, configmap := range configmaplist.Items {
		metaConfigMap, err := meta.Accessor(&configmap)
		if err != nil {
			return nil, err
		}

		controllerRef := metav1.GetControllerOf(metaConfigMap)
		if controllerRef != nil {
			if controllerRef.UID == j.UID {
				configmaps = append(configmaps, &configmaplist.Items[i])
			}
		}
	}
	return configmaps, nil

}

func (qjrConfigMap *QueueJobResConfigMap) getConfigMapForQueueJobRes(qjobRes *arbv1.XQueueJobResource, j *arbv1.XQueueJob) ([]*v1.ConfigMap, error) {

	configmaps, err := qjrConfigMap.getConfigMapForQueueJob(j)
	if err != nil {
		return nil, err
	}

	myConfigMaps := []*v1.ConfigMap{}
	for i, configmap := range configmaps {
		if qjrConfigMap.refManager.BelongTo(qjobRes, configmap) {
			myConfigMaps = append(myConfigMaps, configmaps[i])
		}
	}

	return myConfigMaps, nil

}

func (qjrConfigMap *QueueJobResConfigMap) deleteQueueJobResConfigMaps(qjobRes *arbv1.XQueueJobResource, queuejob *arbv1.XQueueJob) error {

	job := *queuejob

	activeConfigMaps, err := qjrConfigMap.getConfigMapForQueueJobRes(qjobRes, queuejob)
	if err != nil {
		return err
	}

	active := int32(len(activeConfigMaps))

	wait := sync.WaitGroup{}
	wait.Add(int(active))
	for i := int32(0); i < active; i++ {
		go func(ix int32) {
			defer wait.Done()
			if err := qjrConfigMap.delConfigMap(queuejob.Namespace, activeConfigMaps[ix].Name); err != nil {
				defer utilruntime.HandleError(err)
				glog.V(2).Infof("Failed to delete %v, queue job %q/%q deadline exceeded", activeConfigMaps[ix].Name, job.Namespace, job.Name)
			}
		}(i)
	}
	wait.Wait()

	return nil
}

//Cleanup deletes all services
func (qjrConfigMap *QueueJobResConfigMap) Cleanup(queuejob *arbv1.XQueueJob, qjobRes *arbv1.XQueueJobResource) error {
	return qjrConfigMap.deleteQueueJobResConfigMaps(qjobRes, queuejob)
}
