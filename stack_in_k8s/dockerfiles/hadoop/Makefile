.PHONY: build_%

# Current apache hadoop releases: http://hadoop.apache.org/releases.html

HADOOP_33_VERSION = 3.3.3
PROTOBUF_VERSION = 2.5.0

DOCKER = docker

DOCKER_REPO = danisla/hadoop

all: build

### Build hadoop 2.6 and 2.7 images with native libs.
build: build-$(HADOOP_33_VERSION)
build-%: hadoop-native-%.tar
	$(DOCKER) build -t hadoop:$* \
	--build-arg HADOOP_VERSION=$* \
	.

tag: tag-$(HADOOP_33_VERSION)
tag-%:
	$(DOCKER) tag hadoop:$* $(DOCKER_REPO):$*

push: push-$(HADOOP_33_VERSION)
push-%:
	$(DOCKER) push $(DOCKER_REPO):$*

### Fetch source from closest mirror
hadoop-%-src.tar.gz:
	curl -sfL http://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-$*/hadoop-$*-src.tar.gz | \
		egrep -C 3 "We suggest" | \
		perl -n -e'/href="(.*?)"/ && print $$1' | \
		xargs curl -LO

### Fetch binary distribution from closest mirror
hadoop-%.tar.gz:
	curl -sfL http://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-$*/hadoop-$*.tar.gz | \
		egrep -C 3 "We suggest" | \
		perl -n -e'/href="(.*?)"/ && print $$1' | \
		xargs curl -LO

### Fetch protobuf source
protobuf-%.tar.bz2:
	curl -LO https://github.com/google/protobuf/releases/download/v$*/protobuf-$*.tar.bz2

# Keep intermediate downloads.
.PRECIOUS: protobuf-%.tar.bz2 hadoop-%-src.tar.gz hadoop-%.tar.gz

### Compile native libs (~10min)
native_libs_%: hadoop-%-src.tar.gz protobuf-$(PROTOBUF_VERSION).tar.bz2
	$(DOCKER) build -f Dockerfile-compile -t hadoop-nativelibs:$*\
		--build-arg=HADOOP_VERSION=$* \
		--build-arg=PROTOBUF_VERSION=$(PROTOBUF_VERSION) \
		.

### Extract native libs from previous compile target
hadoop-native-%.tar: native_libs_% hadoop-%.tar.gz
	$(DOCKER) run --rm \
	 	-e HADOOP_VERSION=$* \
		hadoop-nativelibs:$* > hadoop-native-$*.tar
