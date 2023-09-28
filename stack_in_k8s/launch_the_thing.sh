docker pull rabbitmq:3.11.4-alpine && \
docker pull busybox:1.28 && \
minikube delete --all --purge && \
minikube start --cpus 8 --memory 30960 --disk-size 20480 --insecure-registry="containeryard.evoforge.org" && \
minikube image load rabbitmq:3.11.4-alpine  && \
minikube image load busybox:1.28 && \
minikube addons enable ingress && \
minikube kubectl -- delete -A ValidatingWebhookConfiguration ingress-nginx-admission && \
minikube kubectl -- patch deployment -n ingress-nginx ingress-nginx-controller --type='json' -p='[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value":"--enable-ssl-passthrough"}]' && \
echo "$(minikube ip) example-ui.datawave.evoforge.org" | sudo tee -a /etc/hosts  && \
echo "$(minikube ip) web.datawave.evoforge.org" | sudo tee -a /etc/hosts && \
echo "$(minikube ip) dictionary.datawave.evoforge.org" | sudo tee -a /etc/hosts && \
helm repo add y341-helm https://artifactory.adv.evoforge.org/artifactory/y341-helm-local/ --username datawave-token  --password "eyJ2ZXIiOiIyIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYiLCJraWQiOiJNM2xWbG1WM1JScHhlaUxPOGkxd1A0b1ZDbmptb1dqMmtoSGF4MU4weTBjIn0.eyJzdWIiOiJqZnJ0QDAxZjI0cXlueWoyaDl5MXJqbjRyNGsweDkzXC91c2Vyc1wvZGF0YXdhdmUtdG9rZW4iLCJzY3AiOiJtZW1iZXItb2YtZ3JvdXBzOmRhdGF3YXZlIGFwaToqIiwiYXVkIjoiamZydEAwMWYyNHF5bnlqMmg5eTFyam40cjRrMHg5MyIsImlzcyI6ImpmcnRAMDFmMjRxeW55ajJoOXkxcmpuNHI0azB4OTNcL3VzZXJzXC9hbGxldmluIiwiaWF0IjoxNjMxNzk2NDIyLCJqdGkiOiIxOGZkNDcwOS1iYjA1LTQwNDQtYjdiNi1mYzEzMTM2NmI4NzEifQ.b6F9h4QND0L0-wk1satdrQKxdFTtqjH2fltWWw63xzXNDPEUAM5ZQ0Z2wDdCoZAWvmxTARXebCc7NPrMpYyXO8yx0VfUZPykl0SZ4Za_bhio72yt_2S1H-kzgHAQeZbB0Jnk0zOOpwckBUxUQMHvVmILk26XlCtJGA2rw8Ox2DER93S9MDuJ0D-oNHqYdsG-I796omxHM244b00Ld3CfDuHZ78xcKTWkmvdF-264S0k2qY8TYGCB37p51i-jvcupPLtq5WOHhsRHMo_Hp7bwGB0ICeEd3EYtOc1tfWfKoeDV6hUZ_FRYV4XILlXO_3APuQ-mjUNlRlJAmwoiPYucHw" && \
helm repo update && \
helm install dwv y341-helm/datawave-system && \
kubectl cp tv-show-raw-data-stock.json dwv-dwv-hadoop-hdfs-nn-0:/tmp && \
kubectl exec -it dwv-dwv-hadoop-hdfs-nn-0 -- hdfs dfs -put /tmp/tv-show-raw-data-stock.json /data/myjson
