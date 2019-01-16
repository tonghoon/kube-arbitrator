Follow the instructions below to deploy the kube-arbitrator extended queuejob in an existing Kubernetes cluster:

Prerequisitis:
- Cluster running Kubernetes v1.10 or highter.
- Access to the `kube-system` namespace.

Installation:
### 1. Download the github project.
```
git clone git@github.com:dmatch01/kube-arbitrator.git
```
### 2. Navigate to the Helm deployment directory.
```
cd kube-arbitrator
git checkout xqueuejob_contrib_helm_fix
cd contrib/DLaaS/deployment
```
### 3. Run the installation Helm chart.

```
helm install kube-arbitrator --namespace kube-system
```
NOTE: You can adjust the cpu and memory demands of the deployment with command line overrides.  For example:
```
helm install kube-arbitrator --namespace kube-system --set resources.requests.cpu=1000m --set resources.requests.memory=1024Mi --set resources.limits.cpu=1000m --set resources.limits.memory=1024Mi
```