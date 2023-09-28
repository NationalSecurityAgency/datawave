### DataWave Stack MiniKube Helm Deployment ###

This repository holds Helm charts and Docker files used to deploy Datawave locally for testing. 


The following instructions assume you do not already have the prerequisites installed (docker, minikube, helm, kubectl). If you do you can simply run `./launch_the_thing.sh`

## First install docker ##
```
sudo yum update
sudo yum install -y yum-utils device-mapper-persistent-data lvm2
sudo yum-config-manager  --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install docker-ce
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker $(whoami)
```

Log out and back in to ensure groups change is picked up

## If using an M5D node, you might need to update Docker storage location like so: ##
```
sudo mount /dev/nvme1n1 /srv
sudo service docker stop
```
Create /etc/docker/daemon.json with the following content:: 
```
{
  "data-root": "/srv/docker"
}
```
```
sudo service docker start
```

## Install Kubectl ##
```
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
```

## Install minikube ##
```
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
```

## Install helm ##
```
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh
```

## Bam start cluster ##
```
./launch_the_thing.sh
```