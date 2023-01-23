### DataWave Stack MiniKube Helm Deploymeny ###

This repository holds Helm charts and Docker files used to deploy Datawave locally for testing. 

The Gitlab CI/CD job that runs will push the charts into the Artifactory helm. Once you have installed helm and minikube, the following commands will start the cluster:

    minikube start --cpus 8 --memory 30960 --disk-size 20480 --insecure-registry="containeryard.evoforge.org"
    helm repo add y341-helm https://artifactory.adv.evoforge.org/artifactory/y341-helm-local/datawave/ --username <user> --password <password> #See artifactory docs or another dev for what login info to use
    helm repo update
    helm install zk y341-helm/zookeeper-minikube
    helm install hadoop y341-helm/hadoop-minikube
    helm install ingest y341-helm/ingest-minikube
    helm install accumulo y341-helm/accumulo-minikube


