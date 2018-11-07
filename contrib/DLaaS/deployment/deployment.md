Follow the instructions below to deploy the kube-arbitrator extended queuejob in an existing Kubernetes cluster:

Prerequisitis:
- Cluster running Kubernetes v1.10 or highter.
- Access to the `kube-system` namespace.

Installation:
### 1. Download the github project.
```
git clone git@github.com:dmatch01/kube-arbitrator.git
cd kube-batch
git xqueuejob_contrib_helm_fix 
```
### 2. Navigate to the Helm deployment directory.
```
cd contrib/DLaaS/deployment
```
### 3. Run the installation Helm chart.

```
helm install kube-arbitrator --namespace kube-system
```