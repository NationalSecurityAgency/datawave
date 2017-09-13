# DataWave Quickstart Docker Image

### Goals

* Provide a containerized, standalone DataWave environment to jumpstart the learning process for new users

* Provide a fully-configured dev environment for developers to use for experimentation, debugging, etc. 
  The Docker container retains all the functionality of the non-containerized quickstart environment (see 
  datawave-quickstart/README.md). For convenience, the container includes Maven for rebuilding DataWave, 
  and Git for source code management

* Enable streamlined testing and integration workflows for various CI/CD needs

---

### Scripts

##### docker-build.sh 

* Creates a Docker image that mirrors the current DataWave source tree under /opt/datawave, including a 
  fully-functional deployment of DataWave under /opt/datawave/contrib/datawave-quickstart

##### docker-run-example.sh

* Example ` docker run ... ` wrapper script, including set up for volumes, port mapping, etc

##### docker-entrypoint.sh

* ENTRYPOINT script for the Docker container

##### datawave-bootstrap.sh

* Helper script for starting up DataWave ingest and web service components within the container, via 
  ` --ingest ` and ` --web ` flags respectively. When invoked with the ` --bash ` flag, the script
  will ` exec /bin/bash ` as the foreground container process (approroiate for ` docker run -it ` usage).
  Without the ` --bash ` flag, the script will go into an infinite loop after starting services
  (more appropriate for ` docker run -d ` usage)

---
