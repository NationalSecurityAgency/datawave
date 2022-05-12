# DATAWAVE Docker Compose

## Components

### Consul

Consul v1.9.8 is a prepacked docker image used for discovery between the various services.

### RabbitMQ

RabbitMQ v3.8.25 is a prepacked docker image used for messaging between the various services.

### Configuration

[Datawave Config Service v1.5-SNAPSHOT](https://github.com/NationalSecurityAgency/datawave-config-service/tree/feature/spring-boot-2.4) is Datawave's customized Spring Cloud config service.

Sample configuration files can be found in the config folder.

You will need to build the docker image for this service on your local machine following the instructions in the config service README.

### Cache

[Datawave Hazelcast Service v1.7-SNAPSHOT](https://github.com/NationalSecurityAgency/datawave-hazelcast-service/tree/feature/spring-boot-2.4) is Datawave's customized Hazelcast In-Memory Data Grid.

You will need to build the docker image for this service on your local machine following the instructions in the hazelcast cache service README.

### Authorization

[Datawave Authorization Service v1.11-SNAPSHOT](https://github.com/NationalSecurityAgency/datawave-authorization-service/tree/feature/spring-boot-2.4) provides basic authorization for the Datawave microservices.

You will need to build the docker image for this service on your local machine following the instructions in the authorization service README.

### Audit

[Datawave Audit Service v1.10-SNAPSHOT](https://github.com/NationalSecurityAgency/datawave-audit-service/tree/feature/spring-boot-2.4) provides query audit capabilities for Datawave.

You will need to build the docker image for this service on your local machine following the instructions in the audit service README.

### Metrics

[Datawave Query Metric Service v1.3-SNAPSHOT](https://github.com/NationalSecurityAgency/datawave-query-metric-service/tree/feature/spring-boot-2.4) provides metrics caching, storage, and retrieval capabilities for Datawave.

You will need to build the docker image for this service on your local machine following the instructions in the query metrics service README.

### Zookeeper

Zookeeper a prepacked docker image used for distributed synchronization.

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

[Datawave Dictionary Service v1.2-SNAPSHOT](https://github.com/NationalSecurityAgency/datawave-dictionary-service/tree/feature/spring-boot-2.4) provides access to the data dictionary and edge dictionary for Datawave.

You will need to build the docker image for this service on your local machine following the instructions in the dictionary service README.

## Usage

### Prereqs

#### /etc/hosts

In order for the following bootstrap step to work properly, you should ensure that your /etc/hosts file looks similar to the following:

```
<your ip address>    <your fqdn> <your hostname>
127.0.0.1            localhost
```

#### Docker

These services have been successfully deployed using the following versions of docker and docker-compose.

```
$> docker --version
Docker version 20.10.3, build 48d30b5
$> docker-compose version
docker-compose version 1.28.4, build cabd5cfb
docker-py version: 4.4.3
CPython version: 3.7.10
OpenSSL version: OpenSSL 1.1.0l  10 Sep 2019
```

#### Datawave Quickstart Setup

Prior to starting these services, you need to use the datawave-quickstart to deploy Hadoop, Zookeeper, and Accumulo on your host machine.  This will also ensure that you have some data available for query.

Before running the quickstart setup, you need to edit your ~/.bashrc to include the following export:

```
export DW_BIND_HOST=0.0.0.0
```

This will ensure that Hadoop binds to all interfaces, and that Accumulo binds to the hostname/IP address.  This is required to connect to the host Accumulo instance from a docker container.

What follows is a brief description of how to setup and run the Datawave Quickstart.  For more information see the [DataWave Quickstart Readme](../../contrib/datawave-quickstart/README.md) for more details.

```
# Add the quickstart env.sh to your .bashrc
# DW_SOURCE refers to your local path to thje datawave source code, and may be set as an environment variable if desired
echo "source DW_SOURCE/contrib/datawave-quickstart/bin/env.sh" >> ~/.bashrc

# Source .bashrc to kick off the quickstart build
source ~/.bashrc

# Install Datawave and it's dependencies
allInstall

# Start Accumulo and it's dependencies
accumuloStart

# At this point, you are ready to deploy and test the query microservices via docker-compose

# If desired, start the wildfly webservice, and run some diagnostic tests
datawaveWebStart && datawaveWebTest

# Make sure to stop the wildfly webservice before starting the query microservices via docker-compose
datawaveWebStop
```

### Bootstrap

The audit, dictionary, and query metric services all need to connect to Zookeeper and Accumulo on the host system.  In order to do that, there are some host-specific environment variables which need to be configured.  

Bootstrap your deployment by running:

```./bootstrap.sh```

This will produce a `.env` file containing the following:

```
DW_HOSTNAME=<Your hostname>
DW_HOST_FQDN=<Your host FQDN>
DW_HOST_IP=<Your host IP Address>
```

### Start services

Start the default services (with the Kafka as the backend):

```docker-compose up -d```

Start the default services (with RabbitMQ as the backend):

```BACKEND=rabbitmq docker-compose up -d```

Start the default services (with Hazelcast as the backend):

```BACKEND=hazelcast docker-compose up -d```

Start the default services, and the dictionary service:

```docker-compose --profile dictionary up -d```

Start the default services, the kafka services, and the dictionary service:

```docker-compose --profile dictionary --profile kafka up -d```

Start all services:

```docker-compose --profile full up -d```

### View logs

For everything:

```docker-compose logs -f```

For a specific service:

```docker-compose logs -f audit```

### Stop services

```docker-compose down```

### Restart a service and pull an updated image
```
docker-compose stop audit
docker-compose rm -f audit
docker-compose up -d
```

### Restart services

```docker-compose restart <servicename>```
