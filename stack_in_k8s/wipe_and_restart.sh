minikube delete --all --purge && \
minikube start --cpus 8 --memory 30960 --disk-size 20480 --insecure-registry="containeryard.evoforge.org" && \
minikube image load rabbitmq:3.11.4-alpine  && \
minikube image load busybox:1.28 && \
minikube addons enable ingress && \
minikube kubectl -- patch deployment -n ingress-nginx ingress-nginx-controller --type='json' -p='[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value":"--enable-ssl-passthrough"}]' && \
echo "$(minikube ip) example-ui.datawave.evoforge.org" | sudo tee -a /etc/hosts  && \
echo "$(minikube ip) web.datawave.evoforge.org" | sudo tee -a /etc/hosts && \
echo "$(minikube ip) dictionary.datawave.evoforge.org" | sudo tee -a /etc/hosts && \
helm repo update && \
helm install dwv y341-helm/datawave-system