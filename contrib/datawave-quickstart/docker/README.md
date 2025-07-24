# DataWave Quickstart Docker Image

### Goals

* Provide a containerized, standalone DataWave environment to jumpstart the learning process for new users

* Provide a fully-configured dev environment for developers to use for experimentation, debugging, etc. That is,
  the Docker image retains the same layout and functionality of the [non-Docker quickstart environment](../README.md).
  For convenience, the image will also include Maven for rebuilding DataWave and Git for source code management

* Enable streamlined testing and integration workflows for various CI/CD needs 

---

### Scripts

Note that the scripts below for building/running the datawave-quickstart container assume that the user executing them is in
the local *docker* group and may thus execute docker commands without requiring *sudo*. For example, to ensure that this is the case:
```bash
$ cat /etc/group | grep -qE '^docker:' || sudo groupadd docker
$ sudo usermod -aG docker yourusername
```

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
