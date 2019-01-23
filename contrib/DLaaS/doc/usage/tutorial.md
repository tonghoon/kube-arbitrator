# Tutorial of Extended QueueJob Controller

This doc will show how to run `xqueuejob-controller` as a kubernetes custom resource definition. It is for [master](https://github.com/kubernetes-sigs/kube-batch/tree/master) branch.

## 1. Pre-condition
To run `xqueuejob-controller`, a Kubernetes cluster must start up. Here is a document on [Using kubeadm to Create a Cluster](https://kubernetes.io/docs/setup/independent/create-cluster-kubeadm/). Additionally, for common purposes and testing and deploying on local machine, one can use Minikube. This is a document on [Running Kubernetes Locally via Minikube](https://kubernetes.io/docs/getting-started-guides/minikube/).

`xqueuejob-controller` need to run as a kubernetes custom resource definition. The next step will show how to run `xqueuejob-controller` as kubernetes custom resource definition quickly. 

## 2. Config kube-batch for Kubernetes

#### Deploy `kube-batch` by Helm

Follow the instructions [here](../../deployment/deployment.md) to install the `xqueuejob-controller` as a custom resource definition using [Helm](../../deployment/deployment.md).

### 3. Create a Job

Create a file named `qj-02.yaml` with the following content:

```yaml
apiVersion: arbitrator.incubator.k8s.io/v1alpha1
kind: XQueueJob
metadata:
  name: helloworld-2
spec:
  schedSpec:
    minAvailable: 2
  resources:
    Items:
    - replicas: 1
      type: StatefulSet
      template:
        apiVersion: apps/v1 # for versions before 1.9.0 use apps/v1beta2
        kind: StatefulSet
        metadata:
          name: helloworld-2
          labels:
            app: helloworld-2
        spec:
          selector:
            matchLabels:
              app: helloworld-2
          replicas: 2 
          template:
            metadata:
              labels:
                app: helloworld-2
                size: "2" 
            spec:
              containers:
               - name: helloworld-2
                 image: gcr.io/hello-minikube-zero-install/hello-node
                 resources:
                   requests:
                     memory: "300Mi"
```

The yaml file means a Job named `helloworld-2` to create 2 pods, these pods will be scheduled by kubernetes scheduler.

Create the Job

```bash
# kubectl create -f qj-02.yaml
```

Check job status

```bash
# kubectl get xqueuejobs
NAME                  CREATED AT
helloworld-2          1h
```

Check the pods status

```bash
# kubectl get pod --all-namespaces
```
