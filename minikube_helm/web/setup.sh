#!/bin/bash

echo "Confirming that Docker CLI is Installed..."
if ! [ -x "$(command -v docker)" ]; then

    echo "Attempting to Install Docker Community Edition (CE)..."
    sudo yum install docker -y

    echo "Retrying to Confirm that Docker CLI is Installed..."
    if ! [ -x "$(command -v docker)" ]; then
        echo "Failed to Install Docker Community Edition (CE)!"
    fi
    
fi

echo "Confirming Availability of Docker Daemon..."
if ! [ "$(docker ps)" ]; then

    echo "Starting and Enabling Docker Service..."
    sudo systemctl enable docker
    sudo systemctl start docker

    echo "Retrying to Confirm Availability of Docker Daemon..."
    if ! [ "$(docker ps)" ]; then
        echo "Failed to Start Local Docker Service!"
    fi

fi

echo "Confirming that Kubectl is Installed..."
if ! [ -x "$(command -v kubectl)" ]; then

    echo "Attempting to Install Kubectl..."
    curl -LO "https://storage.googleapis.com/kubernetes-release/release/v1.19.4/bin/linux/amd64/kubectl"
    chmod +x ./kubectl
    sudo mv ./kubectl /usr/bin/kubectl

    echo "Retrying to Confirm that Kubectl is Installed..."
    if ! [ -x "$(command -v docker)" ]; then
        echo "Failed to Install Kubectl!"
    fi
    
fi

echo "Confirming that Helm is Installed..."
if ! [ -x "$(command -v helm)" ]; then

    curl -LO "https://get.helm.sh/helm-v3.4.1-linux-amd64.tar.gz"
    tar -zxvf helm-v3.4.1-linux-amd64.tar.gz
    sudo mv ./linux-amd64/helm /usr/bin/helm
    rm -rf ./helm-v3.4.1-linux-amd64.tar.gz ./linux-amd64

    echo "Retrying to Confirm that Helm is Installed..."
    if ! [ -x "$(command -v docker)" ]; then
        echo "Failed to Install Helm!"
    fi
    
fi

echo "Confirming that Minikube is Installed..."
if ! [ -x "$(command -v minikube)" ]; then

    curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
    sudo install minikube-linux-amd64 /usr/local/bin/minikube

    echo "Retrying to Confirm that Minikube is Installed..."
    if ! [ -x "$(command -v minikube)" ]; then
        echo "Failed to Install Minikube!"
    fi
    
fi

echo "Confirming that Minikube Cluster is Available..."
minikube start --insecure-registry "10.0.0.0/8" 
