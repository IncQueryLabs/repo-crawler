# Executing Repository Crawler on Docker and Kubernetes

## Docker
### Prerequisites
1. Docker Engine 17.05+ (Tested on 20.10.2)

### Build Repository Crawler application and then package it inside a Docker image
`docker build -t repo-crawler:1.2 .`  
Application will be built inside the first stage image then only the binary is packaged inside the final docker image.

### Executing
Examples:
#### OpenSE Cookbook model on twc.openmbee.org
`docker run -ti --rm repo-crawler:1.2 -S twc.openmbee.org -P 8111 -ssl -W 9c368adc-10cc-45d9-bec6-27aedc80e68b -R c6bede89-cd5e-487b-aca8-a4f384370759 -B 29110c0f-bdc1-4294-ae24-9fd608629cac -REV 350 -C 2000 -u openmbeeguest -pw guest`

## Kubernetes
### Prerequisites
1. Helm 2.x.x+ (Tested on 3.5.3)
2. Docker Engine 17.05+ (Tested on 20.10.2)
3. Kubernetes 1.3.x+ (Tested on 1.19.3) (Must be compatible with Helm)

### Description
The helm chart will execute Repo Crawler as a Job on Kubernetes.

### Features
1. Restart on pod failure
2. Reschedule on node failure
3. Unique instance: Deploy it in the same namespace multiple times
4. Immutable: Cannot be upgraded or modified after deployment

### Configure deployment
Deployment can be configured via `./repo-crawler-chart/values.yaml` file.

Optional fields
- arguments: Arguments to pass to the Repository Crawler application (Default: None)  
  Example: `arguments: ['-W', '9c368adc-10cc-45d9-bec6-27aedc80e68b', '-R', 'c6bede89-cd5e-487b-aca8-a4f384370759', '-B', '29110c0f-bdc1-4294-ae24-9fd608629cac', '-REV', '"350"', '-C', '"2000"', '-u', 'openmbeeguest', '-pw', 'guest']`  
  (Put numbers into "" due to [bug](https://github.com/kubernetes/kubernetes/issues/82296) in Kubernetes)

Optional fields group (All of these should be configured or none):
- serverUri: URI to the Teamwork Cloud 19.0 API
- port: Access port of Teamwork Cloud 19.0 API
- ssl: If application should use SSL to connect to Teamwork Cloud 19.0 API


### Deploy
1. Build Docker image  
   `docker build -t repo-crawler:1.2 .`
2. Upload it to the Docker Registry used in your Kubernetes instance.  
   `docker push your-repository:5000/repo-crawler:1.2`  
   This step can be omitted on desktop Kubernetes deployments e.g.: Docker Desktop
2. Set desired command and target server in `./repo-crawler-chart/values.yaml`
3. Install application with: `helm install repo-crawler ./repo-crawler-chart`

To get logs of you application execute (Bash, PS):  
`kubectl logs $(kubectl get pods --selector=job-name=repo-crawler-job --output=jsonpath='{.items[*].metadata.name}')`

### Uninstall
`helm uninstall repo-crawler`