# Deploying Extended QueueJob
Follow the instructions below to deploy the kube-batch extended queuejob in an existing Kubernetes cluster:

## Pre-Reqs
### - Cluster running Kubernetes v1.10 or highter.
```
kubectl version
```
### - Access to the `kube-system` namespace.
```
kubectl get pods -n kube-system
```
### - Install the Helm Package Manager
Install the Helm Client on your local machine and the Helm Cerver on your kubernetes cluster.  Helm installation documentation is [here]
(https://docs.helm.sh/using_helm/#installing-helm).  After you install Helm you can list the Help packages installed with the following command:
```
helm list
```

## Installation Instructions
### 1. Download the github project.
Download this github project to your local machine.  
```
git clone git@github.com:dmatch01/kube-arbitrator.git
```
### 2. Navigate to the Helm deployment directory.
```
cd kube-arbitrator
git checkout xqueuejob_contrib_helm_fix
cd contrib/DLaaS/deployment
```
### 3. Determine if the cluster has enough resources for installing the Helm chart for the Enhanced QueueJob.

The default memory resource demand for the extended queuejob controller is `2G`.  If your cluster is a small installation such as MiniKube you will want to adjust the Helm installation resource requests accordingly.  


To list available compute nodes on your cluster enter the following command:
```
kubektl get nodes
```
For example:
```
$ kubectl get nodes
     NAME       STATUS    ROLES     AGE       VERSION
     minikube   Ready     master    91d       v1.10.0
```

To find out the available resources in you cluster inspect each node from the command output above with the following command:
```
kubectl describe node <node_name>
```
For example:
```
$ kubectl describe node minikube
...
Name:               minikube
Roles:              master
Labels:             beta.kubernetes.io/arch=amd64
                    beta.kubernetes.io/os=linux
...
Capacity:
 cpu:                2
 ephemeral-storage:  16888216Ki
 hugepages-2Mi:      0
 memory:             2038624Ki
 pods:               110
Allocatable:
 cpu:                2
 ephemeral-storage:  15564179840
 hugepages-2Mi:      0
 memory:             1936224Ki
 pods:               110
...
Allocated resources:
  (Total limits may be over 100 percent, i.e., overcommitted.)
  Resource  Requests      Limits
  --------  --------      ------
  cpu       1915m (95%)   1 (50%)
  memory    1254Mi (66%)  1364Mi (72%)
Events:     <none>

```
In the example above, there is only one node (`minikube`) in the cluster with the majority of the cluster memory used (`1,254Mi` used out of `1,936Mi` allocatable capacity) leaving less than `700Mi` available capacity for new pod deployments in the cluster.  Since the default memory demand for the Enhanced QueueuJob Controller pod is `2G` the cluster has insufficient memory to deploy the controller.  Instruction notes provided below show how to override the defaults according to the available capacity in your cluster.

### 4. Run the installation using Helm.
Install the Extended QueueJob Controller using the command below.  If you do not have enough compute resources in your cluster you can adjust the resource request via the command line.  See an example in the `Note` below.  
```
helm install kube-arbitrator --namespace kube-system
```
NOTE: You can adjust the cpu and memory demands of the deployment with command line overrides.  For example:
```
helm install kube-arbitrator --namespace kube-system --set resources.requests.cpu=1000m --set resources.requests.memory=1024Mi --set resources.limits.cpu=1000m --set resources.limits.memory=1024Mi
```

### 5. Verify the installation.
List the Helm installations.
```
helm list
```

List the Extended QueueJobs
```bash
kubectl get xqueuejobs
```

Since no `xqueuejobs` have been deploy yet to your cluster you should receive a message indicating `No resources found.` for `xqueuejobs` but your cluster now has `xqueuejobs` enabled.  Use the [tutorial](../doc/usage/tutorial.md) to deploy an example `xqueuejob`.

### 6.  Remove the Extended QueueJob Controller from your cluster.

List the deployed Helm charts and identify the name of the Extended QueueJob Controller installation.
```bash
helm list
```
For Example
```
$ helm list
NAME                	REVISION	UPDATED                 	STATUS  	CHART                	NAMESPACE  
opinionated-antelope	1       	Mon Jan 21 00:52:39 2019	DEPLOYED	kube-arbitrator-0.1.0	kube-system

```
Delete the Helm deployment.
```bash
helm delete <deployment_name>
```
For example:
```bash
helm delete opinionated-antelope
```
