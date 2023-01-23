minikube delete --all --purge && \
minikube start --cpus 8 --memory 30960 --disk-size 20480 --insecure-registry="containeryard.evoforge.org" && \
minikube image load busybox:1.28
minikube addons enable ingress 
