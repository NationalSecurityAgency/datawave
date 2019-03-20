# DataWave Quickstart Docker Image

### Goals

* Provide a containerized, standalone DataWave environment to jumpstart the learning process for new users

* Provide a fully-configured dev environment for developers to use for experimentation, debugging, etc. That is,
  the Docker image retains the same layout and functionality of the [non-Docker quickstart environment](../README.md).
  For convenience, the image will also include Maven for rebuilding DataWave and Git for source code management

* Enable streamlined testing and integration workflows for various CI/CD needs 

---

### Recommended Docker Configuration (Optional)

* Testing was performed on Linux with **Docker 18.09.3-ce, build 774a1f4**, with Docker
  configured as follows: 
  ```
  $ cat /etc/docker/daemon.json
  {
    "storage-driver": "devicemapper",
    "experimental": true
  }
  ```
* The *devicemapper* storage driver can be used as a workaround for certain issues that may arise during the Wildfly
  deployment (i.e., during the docker image build). See https://issues.jboss.org/browse/WFCORE-2301
  
* Experimental mode can be used to enable features such as ` docker build --squash ` to help minimize the size of
  the resulting image

---

### Scripts

Note that the scripts below for building/running the datawave-quickstart container assume that the user executing them is in
the local *docker* group and may thus execute docker commands without requiring *sudo*. For example, to ensure that this is the case:
```bash
$ cat /etc/group | grep -qE '^docker:' || sudo groupadd docker
$ sudo usermod -aG docker yourusername
```
*Warning*: Executing `sudo ./docker-build.sh` will likely result in file permission issues in both your DataWave source tree and your
local Maven artifact repository, and will ultimately cause Docker image creation to fail

#### [docker-build.sh](docker-build.sh) 

* Uses [Dockerfile](Dockerfile) to create a Docker image that mirrors the current DataWave source tree under the image's
  /opt/datawave root directory, including a fully-initialized deployment of DataWave under /opt/datawave/contrib/datawave-quickstart.
  In other words, the image has the exact same layout as the non-Docker quickstart environment
  
* Use the script's `--docker-opts` param to pass additional options thru to ` docker build ...` as needed

* Get usage info with ` ./docker-build.sh --help `

#### [docker-run.sh](docker-run.sh)

* Wrapper script for running the quickstart container via ` docker run ... `. Includes set up for volumes, port mappings, etc

#### [docker-entrypoint.sh](docker-entrypoint.sh)

* ENTRYPOINT script for the Docker container

#### [datawave-bootstrap.sh](datawave-bootstrap.sh)

* Helper script for starting up DataWave ingest and web service components in the container, via 
  ` --ingest ` and ` --web ` flags respectively. When invoked with the ` --bash ` flag, the script
  will ` exec /bin/bash ` as the main container process, appropriate for ` docker run -it ` usage.
  Without the ` --bash ` flag, the script will go into an infinite loop after starting services,
  more appropriate for ` docker run -d ` usage

---
