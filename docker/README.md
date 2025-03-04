# DATAWAVE Docker Compose

It is recommended to read through these instructions in their entirety before attempting to build or deploy Datawave.  However, 
if you just want to get started and use this document as a reference, here's the short version (although we recommend checking 
out the [prereqs](#prereqs) at a minimum):

## TLDR

```shell
# build docker images for datawave and all of the microservices
# optionally include '-Dquickstart-maven' to download accumulo/zookeeper/hadoop/maven tarballs from the maven repository
mvn -Pcompose -Dservices -Dmicroservice-docker -Dquickstart-docker -Ddeploy -Dtar -Ddist -DskipTests clean install

# bootstrap the services, and bring them up using docker compose
cd docker
./bootstrap.sh
docker compose up -d

# run some queries to ensure everything is working
cd scripts
./testAll.sh
```

## Components

### Quickstart

Datawave Quickstart is a self-contained hadoop, zookeeper, and accumulo deployment prepopulated with data.

### Consul

Consul v1.15.4 is a prepacked docker image used for discovery between the various services.

### RabbitMQ

RabbitMQ v3.12.4 is a prepacked docker image used for messaging between the various services.

### Configuration

[Datawave Config Service](https://github.com/NationalSecurityAgency/datawave-config-service/tree/main) is Datawave's customized Spring Cloud config service.

Sample configuration files can be found in the config folder.

You will need to build the docker image for this service on your local machine following the instructions in the config service README.

### Cache

[Datawave Hazelcast Service](https://github.com/NationalSecurityAgency/datawave-hazelcast-service/tree/main) is Datawave's customized Hazelcast In-Memory Data Grid.

You will need to build the docker image for this service on your local machine following the instructions in the hazelcast cache service README.

### Authorization

[Datawave Authorization Service](https://github.com/NationalSecurityAgency/datawave-authorization-service/tree/main) provides basic authorization for the Datawave microservices.

You will need to build the docker image for this service on your local machine following the instructions in the authorization service README.

### Audit

[Datawave Audit Service](https://github.com/NationalSecurityAgency/datawave-audit-service/tree/main) provides query audit capabilities for Datawave.

You will need to build the docker image for this service on your local machine following the instructions in the audit service README.

### Metrics

[Datawave Query Metric Service](https://github.com/NationalSecurityAgency/datawave-query-metric-service/tree/main) provides metrics caching, storage, and retrieval capabilities for Datawave.

You will need to build the docker image for this service on your local machine following the instructions in the query metrics service README.

### Zookeeper

Zookeeper is a prepacked docker image used for distributed synchronization.

### Kafka

Kafka is a prepacked docker image used for messaging between the various services.

### Query

Datawave Query Service v1.0-SNAPSHOT is a user-facing interface for Datawave query.

This microservice is in development, and can be found in this repo. 

You will need to build the docker image for this service on your local machine following the instructions in the config service README.

### Executor Pool 1

Datawave Executor Service v1.0-SNAPSHOT is the back-end worker for Datawave queries.

This microservice is in development, and can be found in this repo.

You will need to build the docker image for this service on your local machine following the instructions in the config service README.

### Executor Pool 2

Enabled via the 'pool2', or 'full' profile.

Datawave Executor Service v1.0-SNAPSHOT is the back-end worker for Datawave queries.

This microservice is in development, and can be found in this repo.

You will need to build the docker image for this service on your local machine following the instructions in the config service README.

### Query Storage

Enabled via the 'storage', or 'full' profile.

Datawave Query Storage Service v1.0-SNAPSHOT is a utility service used to inspect the storage cache.

This microservice is in development, and can be found in this repo.

You will need to build the docker image for this service on your local machine following the instructions in the config service README.

## Optional Components

### Kafdrop

Enabled via the 'management', or 'full' profile.

Kafdrop is a prepacked docker image used for kafka cluster management.

### Hazelcast Management Center

Enabled via the 'management', or 'full' profile.

Hazelcast Management Center v4.2021.06 is a prepacked docker image used for hazelcast cluster management.

### Dictionary

Enabled via the 'dictionary', or 'full' profile.

[Datawave Dictionary Service](https://github.com/NationalSecurityAgency/datawave-dictionary-service/tree/main) provides access to the data dictionary and edge dictionary for Datawave.

You will need to build the docker image for this service on your local machine following the instructions in the dictionary service README.

### File Provider

Enabled via the 'file-provider', or 'full' profile.

This microservice is in development, and can be found in this repo.

[Datawave File Provider Service](https://github.com/NationalSecurityAgency/datawave-file-provider-service/tree/main) provides file management and access to Datawave and it's services.

You will need to build the docker image for this service on your local machine following the instructions in the file provider service README.


## Usage

Please read through these instructions in their entirety before attempting to build or deploy Datawave.

### Prereqs

#### /etc/hosts

In order for the following bootstrap step to work properly, you should ensure that your /etc/hosts file looks similar to the following:

```
<your ip address>    <your fqdn> <your hostname>
127.0.0.1            localhost
```

#### Docker

These services have been successfully deployed using the following versions of docker and docker compose.

```
$> docker --version
Docker version 24.0.6, build ed223bc
$> docker compose version
Docker Compose version v2.21.0
```

#### Datawave Quickstart

Prior to starting docker compose, you need to use the Datawave Quickstart to deploy working instances of Hadoop, Zookeeper, and Accumulo, along with some sample datasets for query.

There are two methods for deploying the Datawave Quickstart.  

 - **default**: Deploys the Datawave Quickstart as a docker container within docker compose.

 - **hybrid**: Deploys the Datawave Quickstart directly on your host system.

#### Default Datawave Quickstart Setup

Build the Datawave Quickstart docker image using the following build command:

```
# To build the quickstart docker image, and all of the microservice images, run this
mvn -Pcompose -Dservices -Dmicroservice-docker -Dquickstart-docker -Ddeploy -Dtar -Ddist -DskipTests clean install -T1C

# To build just the quickstart docker image, run this
mvn -Pcompose -Dquickstart-docker -Ddeploy -Dtar -Ddist -DskipTests clean install -T1C
```
Note that the quickstart-docker property is set.  This property is a shortcut which activates the `docker` and `quickstart` profiles without activating the `docker` profile for the microservices.

For this command, the build profile is set to `compose`.  This profile contains all of the properties needed to make the quickstart work as part
of the docker compose deployment.  The use of any other build profile with docker compose is unsupported.

This command also prevents the microservice services from building with `-DskipServices`.  This is an optional setting which will skip the microservice builds entirely, saving you some time if you only want to build/rebuild the Datawave Quickstart.  

If you ever need to rebuild the Datawave quickstart docker image, but don't want to ingest the sample data you can add `-DskipIngest` to 
your build command.  This can save you some time, since the docker compose configuration stores ingested data in a persistent volume.

If desired, you can start and test the wildfly deployment embedded in the Datawave Quickstart by running the following command:
```
docker run -m 8g datawave/quickstart-compose datawave-bootstrap.sh --test
```

#### Hybrid Datawave Quickstart Setup

Before running the quickstart setup, you need to edit your ~/.bashrc to include the following export:

```
export DW_BIND_HOST=0.0.0.0
```

This will ensure that Hadoop binds to all interfaces, and that Accumulo binds to the hostname/IP address.  This is required to connect to the host Accumulo instance from a docker container.

What follows is a brief description of how to setup and run the Datawave Quickstart.  For more detailed information see the [DataWave Quickstart Readme](../contrib/datawave-quickstart/README.md).

```
# Add the quickstart env.sh to your .bashrc
# DW_SOURCE refers to your local path to the datawave source code, and may be set as an environment variable if desired
echo "activateDW() {\n source DW_SOURCE/contrib/datawave-quickstart/bin/env.sh\n}" >> ~/.bashrc

# Source .bashrc to kick off the quickstart build
source ~/.bashrc

# Activate DataWave
activateDW

# Install Datawave and its dependencies
allInstall

# Start Accumulo and its dependencies
accumuloStart

# At this point, you are ready to deploy and test the query microservices via docker compose

# If desired, start the wildfly webservice, and run some diagnostic tests
datawaveWebStart && datawaveWebTest

# Make sure to stop the wildfly webservice before starting the query microservices via docker compose
datawaveWebStop
```

#### Datawave Microservices

If you haven't done so already, you can build the Datawave Microservice docker images using the following build command:

```
mvn -Pcompose -Dservices -Dmicroservice-docker -Ddist -DskipTests clean install -T1C
```

Note that the microservice-docker property is set.  This property is a shortcut which activates the `docker` profile for just the microservices.

This command can be combined with default Datawave Quickstart build command to build everything at once.

### Bootstrap

The audit, dictionary, query executor, and query metric services all need to connect to Zookeeper, Hadoop and/or Accumulo.  In order to make that work, there are some environment variables which need to be configured.  

#### Default Bootstrap

Bootstrap your deployment by running:

```./bootstrap.sh```

This will produce a `.env` file containing the following:

```
# If set to quickstart, enables the quickstart container
# Note: More than one profile may be set.
COMPOSE_PROFILES=""

# These environment variables are used to create extra hosts which
# allow containers to route to the host quickstart deployment.
# The extra hosts aren't used when deploying the docker quickstart,
# but the variables still need to be set for the compose file to be valid.
DW_HOSTNAME="<Your hostname>"
DW_HOST_FQDN="<Your host FQDN>"
DW_HOST_IP="<Your host IP Address>"

# These environment variables must be set when running the quickstart
# from the host machine in hybrid mode.
DW_ZOOKEEPER_HOST="<Your hostname>"
DW_HADOOP_HOST="<Your hostname>"
```

#### Hybrid Bootstrap

Bootstrap your deployment by running:

```./bootstrap.sh hybrid```

This will produce a `.env` file containing the following:

```
# If set to quickstart, enables the quickstart container
# Note: More than one profile may be set.
COMPOSE_PROFILES=""

# These environment variables are used to create extra hosts which
# allow containers to route to the host quickstart deployment.
# The extra hosts aren't used when deploying the docker quickstart,
# but the variables still need to be set for the compose file to be valid.
DW_HOSTNAME="<Your hostname>"
DW_HOST_FQDN="<Your host FQDN>"
DW_HOST_IP="<Your host IP Address>"

# These environment variables must be set when running the quickstart
# from the host machine in hybrid mode.
DW_ZOOKEEPER_HOST="<Your hostname>"
DW_HADOOP_HOST="<Your hostname>"
```

### Start services

Start the default services (with the Kafka as the backend):

```docker compose up -d```

Start the default services (with RabbitMQ as the backend):

```BACKEND=rabbitmq docker compose up -d```

Start the default services (with Hazelcast as the backend):

```BACKEND=hazelcast docker compose up -d```

Start the default services, and the dictionary service:

```docker compose --profile quickstart --profile dictionary up -d```

Start the default services, the kafka services, and the dictionary service:

```docker compose --profile quickstart --profile dictionary --profile kafka up -d```

Start the default services, and the file provider service:

```docker compose --profile quickstart --profile file-provider up -d```

Start all services:

```docker compose --profile quickstart --profile full up -d```

### View logs

For everything:

```docker compose logs -f```

For a specific service:

```docker compose logs -f audit```

### Stop services

Stop the configured services

```docker compose down```

Stop the configured services, and delete all volumes

```docker compose down -v```

Stop all services, including ones that are no longer enabled

```docker compose down --remove-orphans```

### Restart a service and pull an updated image

```
docker compose stop audit
docker compose rm -f audit
docker compose up -d
```

### Restart a service without pulling an updated image

```docker compose restart <servicename>```
