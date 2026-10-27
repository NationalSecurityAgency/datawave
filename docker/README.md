# DATAWAVE Docker Compose

It is recommended to read through these instructions in their entirety before attempting to build or deploy Datawave.  However, 
if you just want to get started and use this document as a reference, here's the short version (although we recommend checking 
out the [prereqs](#prereqs) at a minimum):

## System Environment
The following versions are known to work.  
RHEL 9.6+  
Docker 28.5.1+  

RHEL 8 does not seem to work. It interferes with the Docker daemon during the image build.

## TLDR

```shell
# build docker images for datawave and all of the microservices
mvn -T1C -Pcompose -Dservices -Dmicroservice-docker -Ddocker-release -Ddeploy -Dtar -Ddist -DskipTests -Djkube.container-image.tags.1=latest clean install

# bootstrap the services, and bring them up using docker compose
cd docker
./bootstrap.sh
docker compose up -d --wait --wait-timeout 600

# run some queries to ensure everything is working
cd scripts
./testAll.sh
```

## Components

### DataWave Stack Accumulo

[datawave-stack-accumulo](https://github.com/NationalSecurityAgency/datawave-stack-accumulo) supplies the Hadoop and Accumulo runtime. Docker Compose starts its individual services and the `ingest` service loads the same sample datasets used by the DataWave quickstart tests.

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

ZooKeeper is provided by the `datawave-stack-accumulo` image and used for distributed synchronization.

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

#### DataWave Stack Setup

The default Compose profile starts Hadoop, ZooKeeper, and Accumulo from the published `datawave-stack-accumulo` image. It also starts the DataWave ingest and webservice images and loads the sample Wikipedia, annotation, JSON, and CSV data used by the existing test suites.

Build the DataWave images using the following command. The infrastructure image is pulled and does not need to be rebuilt in this repository:

```
mvn -T1C -Pcompose -Dservices -Dmicroservice-docker -Ddocker-release -Ddeploy -Dtar -Ddist -DskipTests -Djkube.container-image.tags.1=latest clean install
```

For this command, the build profile is set to `compose`. This profile contains the properties needed by the Compose deployment. The use of any other build profile with Docker Compose is unsupported.

You may update the property `jkube.container-image.tags.1` to be something other than `latest`, but must use the `VERSION` environment variable with `docker compose` to specify the tag used for DataWave images. Otherwise, Docker Compose uses `latest`.

If you performed a build with the `jkube.container-image.tags.1` property set to anything but `latest`, you can specify the the tag to use when running `docker compose`
```shell
VERSION=20260326.1 docker compose up
```

#### Datawave Microservices

If you haven't done so already, you can build the Datawave Microservice docker images using the following build command:

```
mvn -Pcompose -Dservices -Dmicroservice-docker -Ddist -DskipTests -DskipITs clean install -T1C
```

Note that the microservice-docker property is set.  This property is a shortcut which activates the `docker` profile for just the microservices.

This command can be combined with the DataWave ingest and webservice image build as shown above.

### Bootstrap

The audit, dictionary, query executor, and query metric services all need to connect to Zookeeper, Hadoop and/or Accumulo.  In order to make that work, there are some environment variables which need to be configured.  

#### Default Bootstrap

Bootstrap your deployment by running:

```./bootstrap.sh```

This will produce a `.env` file containing the following:

```
# Enables the reusable Hadoop/Accumulo stack and DataWave fixture loader
# Note: More than one profile may be set.
COMPOSE_PROFILES="datawave-stack"

# These environment variables are used to create extra hosts which
# allow containers to route to services running on the host when using hybrid mode.
DW_HOSTNAME="<Your hostname>"
DW_HOST_FQDN="<Your host FQDN>"
DW_HOST_IP="<Your host IP Address>"

DW_ZOOKEEPER_HOST="zookeeper"
DW_HADOOP_HOST="hdfs-nn"
DW_YARN_HOST="yarn-rm"
HADOOP_CONF_SOURCE="./stack"
```

#### Hybrid Bootstrap

Bootstrap your deployment by running:

```./bootstrap.sh hybrid```

This will produce a `.env` file containing the following:

```
# No infrastructure profile is selected because the backend is running on the host.
COMPOSE_PROFILES=""

# These environment variables are used to create extra hosts which
# allow containers to route to the host backend deployment.
DW_HOSTNAME="<Your hostname>"
DW_HOST_FQDN="<Your host FQDN>"
DW_HOST_IP="<Your host IP Address>"

# Backend service locations for hybrid mode.
DW_ZOOKEEPER_HOST="<Your hostname>"
DW_HADOOP_HOST="<Your hostname>"
DW_YARN_HOST="<Your hostname>"
HADOOP_CONF_SOURCE="<HADOOP_CONF_DIR from your host deployment>"
```

Hybrid mode requires either `HADOOP_CONF_DIR` or `HADOOP_CONF_SOURCE` to point to the host deployment's Hadoop configuration directory when `bootstrap.sh hybrid` runs. The directory is mounted at `/etc/hadoop/conf` inside the service containers.

### Start services

Start the default services (with the Kafka as the backend):

```docker compose up -d```

Start the default services (with RabbitMQ as the backend):

```BACKEND=rabbitmq docker compose up -d```

Start the default services (with Hazelcast as the backend):

```BACKEND=hazelcast docker compose up -d```

Start the default services and the dictionary service:

```docker compose --profile datawave-stack --profile dictionary up -d```

Start the default services, the kafka services, and the dictionary service:

```docker compose --profile datawave-stack --profile dictionary --profile kafka up -d```

Start the default services, and the file provider service:

```docker compose --profile datawave-stack --profile file-provider up -d```

Start all services:

```docker compose --profile datawave-stack --profile full up -d```

### Shard table splits

`stack/shard-splits.txt` holds the split points for `datawave.shard`, one full `yyyyMMdd_num` shard id
per line. `stack/initialize-datawave.sh` applies them with `addsplits` before any data is written, so
the sharded schema starts out spread over one tablet per shard row instead of sitting in a single
tablet. It also writes the `num_shards` entry the query side reads out of `datawave.metadata` to
expand a day from the global index into that day's shards.

The dates are those of the fixture data, and the shard numbers must cover `0` through
`table.shard.numShardsPerDay - 1` from `properties/compose.properties`. Changing that property means
regenerating the file and rebuilding the ingest image, since the count is baked into the image's
ingest configuration.

To check a running stack:

```docker/scripts/verifySplits.sh```

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
