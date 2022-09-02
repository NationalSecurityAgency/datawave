## Customized Hadoop base image

If you want to compile additional native libraries, like [Protobuf](https://wiki.apache.org/hadoop/ProtocolBuffers), you can do so with the provided build targets.

This `Makefile` will compile the native libraries and build the latest Hadoop `2.6` and `2.7` docker images for use with the Kubernetes manifests.

> Note that there isn't anything really unique about the docker image, as the K8S ConfigMap does most of the boostraping and is designed to work with generic Hadoop docker images.

Build Hadoop `2.6` and `2.7` images:

```
make
```

Tag and push images to your registry:

```
DOCKER_REPO=danisla/hadoop make -e tag push
```

## Testing with minikube

If you are running locally with minikube and want to try your images without pushing them to a registry, build the images on the minikube VM first:

```
eval $(minikube docker-env)
make && make tag
```
