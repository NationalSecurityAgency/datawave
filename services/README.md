# DATAWAVE External Services

This module contains DATAWAVE external services. These are microservices that
are intended to work in conjunction with, and eventually replace, the Wildfly
based DATAWAVE web service.

DATAWAVE microservices are built on top of [Spring Cloud](http://cloud.spring.io/spring-cloud-static/Greenwich.RELEASE/single/spring-cloud.html)
and [Spring Boot](https://docs.spring.io/spring-boot/docs/2.1.3.RELEASE/reference/htmlsingle/).

## Why Microservices?

The Wildfly DATAWAVE web service has become a monolith over time, and for
production use, different components of the service evolve at different
rates. However, given the monolithic nature of the service and impact of
outages, deployments are not as frequent as they could be. Splitting the
web service into microservices will allow components to evolve at different
rates. It will also allow components to be scaled independently as the
need arises.


## Features

### Externalized Configuration

Configuration for DATAWAVE microservices is externalized. Services download
their configuration from a configuration service at startup time, and are
also capable of notification of configuration changes via direct contact or
notification over an AMQP message bus. Upon such notification, the services
will reload their configuration dynamically. Configuration is profile-based
and stored in YAML files.

### Authorization Service

The authorization service provides cached authentication and authorization
via either X509 certificates or signed JSON Web Tokens (which themselves can
be retrieved from the authorization service). The authorization service uses
the supplied authentication credentials in order to contact a back end
authorization provider to retrieve associated roles and compute Accumulo
authorizations based on those roles. This information is all collected in a
DatawaveUser object that is returned as the payload in a JWT.

Note that, by default, the authorization service expects to authorize an
entity on behalf of another caller. That is, it is assumed that another
service (such as the existing DATAWAVE Wildfly web service) is authenticating
on behalf of a calling user (or chain of calling user and intermediate
servers). In this scenario, the request is made using the web service's
credentials and therefore that information should not be included in the
response. Practically, this means that if you wish to test the authorization
service directly, you must pass X-ProxiedEntitiesChain/X-ProxiedIssuersChain
headers. It is possible to configure the service to run in a mode where these
headers are not required and then any call without these headers acts as
though the calling server is proxying for itself. This is controlled by setting
`security.proxied-entities-required` to `true`.

### Common Service Starter

Over time, the goal is to keep breaking up the monolithic DATAWAVE web service
into smaller microservices. The `spring-boot-starter-datawave` artifact provides
common utilities and configuration that are intended to be used with any additional 
microservices. Depending on this artifact will activate the contained configuration.
In particular, this applies Spring Security configuration that sets up the service
to accept a signed JSON Web Token (JWT) for authentication and authorization.
Typically some gateway service will take care of calling the authorization
service and retrieving a JWT that represents the calling user chain (this could
even be performed by a load balancer). The JWT will be passed to your service,
and the JWT security configuration will read the JWT header, check the 
signature, and convert the token into a security principal that can be accessed
by your service. This principal contains a list of the proxied `DatawaveUser`
objects which themselves contain the original roles returned by the back end
authorization provider, the computed Accumulo authorizations, and the mapping
from roles to authorizations. If your service needs to call another microservice,
it can take advantage of the Spring RestTemplate configuration provided by the
common module. This configuration sets up RestTemplate so that outgoing calls
will be secured using the configured client certificate, and the authenticated
principal will automatically be converted back to a JWT and passed along in a
header, thus satisfying the authentication and authorization needs of the
service you are calling. With this approach, authentication and authorization
happens once and then the credentials are passed around securely to each
service that is invoked along the calling chain.

### Audit Service

The audit service provides query auditing capabilities to send audit records
to a file or Accumulo. This service, which itself extends the common service
base (and therefore accepts JWTs for authentication), could be extended to
allow the audit records to be sent somewhere else as well (a remote audit
store, for example).

Internally, the audit service simply receives audit messages, validates them,
and then puts the message onto Spring Cloud streams to handle each of the
configured audit sinks (e.g., file and Accumulo).

**Audit Client Starter**

Query services that support auditing integrate with the audit service via the
*spring-boot-starter-datawave-audit* module. View the starter
[README](spring-boot-starter-datawave-audit/README.md) for details.

### Dictionary Service

The dictionary service provides access to the data and edge dictionaries that
were previously contained in the Wildfly monolith. This service extends the
common service base, and therefore accepts JWTs for authentication. If the
`remoteauth` profile is activated, then the service will also attempt to contact
the authorization service in order to authenticate a user when a client
certificate is provided and no JWT is provided.

### Accumulo Service

The Accumulo service is an administrator utility that provides a simple rest
API for performing basic table and security operations on Accumulo. View the
service [README](accumulo-service/README.md) for details.

### Service Discovery

[Consul](https://www.consul.io) is used for service discovery (unless running
in some container orchestrator such as Kubernetes, where DNS would be used
for service discovery). It is somewhat like Zookeeper in that it runs a quorum
of servers to maintain consistency and provide fault tolerance. A lightweight
agent acts as a proxy to the quorum of servers, and also provides DNS-based
discovery of services (Consul also supports an HTTP discovery interface, which
Spring uses behind the scenes). Consul integrates well with Spring Boot /
Spring Cloud. When enabled, each service automatically registers itself with
Consul. When using Spring's RestTemplate to invoke a service, the location of
that service is automatically discovered through Consul. Even the configuration
server, which is needed at startup by all services, registers itself through
Consul and each service during bootstrap contacts Consul to locate the
configuration server and then continues to download the configuration from the
located service.

The DNS-based service discovery is useful for non-Spring applications. For
example, the Wildfly-based DATAWAVE web service needs to contact microservices.
By running Consul on port 53, only the name of the service needs to be 
configured and lookup will happen automatically without any additional glue
code. Or, if Consul is run on its default port of 8600, a small amount of
glue code can be used to contact the non-standard port and locate the service.
Consul also supports SVR DNS records, which not only supply the location of a
service but the port on which it is running as well.

### Shared Caching

While the authorization service may run perfectly well as a single instance and
not need to be scaled to handle load, it would still be desirable to run more
than one copy for fault tolerance. However, if we do that, we then have the
potential for cache inconsistencies (and performance impacts) where a lookup
from one copy of the authorization service returns one result and then a lookup
from another either has to wait to call the backend authorization provider or
returns a cached result that is different and/or has a different lifespan
since it was inserted into the cache at a different time.

Hazelcast provides a client-server cache where the server is a cluster of cache
services that store data and provide it to clients. By running more than a
single cache server, we can achieve fault tolerance for the cache (and even
perform a rolling upgrade). The authorization service uses a Hazelcast client
to connect to the server to store and retrieve cached data. The Hazelcast
client can be configured with a "Near Cache" which stores frequently used data
in-process to avoid the extra network call, if performance becomes an issue.
Note that all of this is hidden behind standard Spring cache abstractions, so
the implementation could be change if the need arises.

### Refreshable Configuration

Spring Boot supports the creation of beans that are annotated with the `RefreshScope`
annotation. This creates a bean that is wrapped with a proxy to the real bean.
Then, upon refresh events, the real bean is re-created thus changing the view
behind the proxied bean. This setup is very similar to the `RefreshableScope`
annotation that is used in the Wildfly-based DATAWAVE web service. As with
that service, only beans that have been annotated as refreshable will benefit
from the mechanism, and one must be careful in a bean that is not refreshable
(but uses a refreshable configuration bean) not to pull values from a 
refreshable bean and store them in member variables. This defeats the purpose
of having a refresh mechanism.

There are three ways to issue a refresh:
1. Send a POST message with an empty body to `/<servicename>/mgmt/refresh` on the
   service you wish to refresh. This will cause all beans annotated wih `RefreshScope`
   to be re-created behind their proxies.
2. Send a POST message with an empty body to `/<servicename>/mgmt/bus-refresh?destination=<otherservicename>:**`
   on any service. This will cause all running instances of `<otherservice>` that are
   listening on the event bus to refresh.
3. Send a POST message with an empty body to `/<servicename>/mgmt/bus-refresh` on
   any service when using RabbitMQ. The service in question and all other services
   using RabbitMQ will be refreshed.
   
# Getting Started

The quickest way to get stared is as follows. First, copy the `.example` files
in the `sample_configuration` directory to the same name without `.example`
(e.g., `authorization-dev.yml.example` becomes `authorization-dev.yml`). Edit
the files you copied and customize any properties you wish.

```bash
cd sample_configuration
for f in *.yml.example; do
    cp ${f} ${f%.example}
done
```

Next, build the DATAWAVE microservices.

```bash
cd /path/to/datawave/services/build-parent
mvn -Pexec clean install
# You can add -DskipTests to skip running unit tests
# You can add -Pdocker to build Docker images
```

Now launch the configuration service. Specify the `sample_configuration` for the
configuration repository.

```bash
cd /path/to/datawave/services
java -jar config-service/target/config-service*-exec.jar --spring.profiles.active=dev,nomessaging,native,open_actuator --spring.cloud.config.server.native.searchLocations=file://$PWD/sample_configuration 
```

Now launch the authorization service.

```bash
cd /path/to/datawave/services
java -jar authorization-service/target/authorization-service*-exec.jar --spring.profiles.active=dev,nomessaging,mock 
```

Note that the authorization service is configured for two-way authentication, and the PKI materials located [here](spring-boot-starter-datawave/src/main/resources) are used by default (password for all: *ChangeIt*). For example, to access the authorization service endpoints below, simply import either the **testUser.p12** or **testServer.p12** client cert into your browser or preferred HTTP client. The default PKI configuration is provided for testing purposes only and is not intended for production use.

Once all services are running, you should be able to hit some of the 
following URLs:
* `https://localhost:8643/authorization/v1/authorize` will return a JWT corresponding
  to your user
* `https://localhost:8643/authorization/v1/whoami` will return a JSON-encoded version
  of the DatawaveUser corresponding to your client certificate
* `https://localhost:8643/authorization/swagger-ui.html` shows Swagger documentation
  of the service
* `https://localhost:8643/authorization/mgmt/` shows available Spring Boot Actuator management
  endpoints

You may see an exception from either the authorization or config service due to
an "Illegal key size". If you see this exception, it means you JRE/JDK does not
have the Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction
Policy files installed. See [here](http://cloud.spring.io/spring-cloud-static/Greenwich.RELEASE/single/spring-cloud.html#_cloud_native_applications)
for more information.

Now launch the dictionary service, if desired:
```bash
cd /path/to/datawave/services
java -jar dictionary-service/target/dictionary-service*-exec.jar --spring.profiles.active=dev,nomessaging,remoteauth 
```
You should be able to retrieve the data and edge dictionaries at the following URLs:
* `https://localhost:8843/dictionary/data/v1/`
* `https://localhost:8843/dictionary/edge/v1/`

If you are invoking from a browser, be sure the PKI materials referenced above have
been loaded into the browser so that the client certificate is sent to the dictionary
service (and then along to the authorization service). Alternatively, you can retrieve
a JWT from the authorization service and pass it along to the dictionary service. The
following example assumes the PKI materials from spring-boot-starter-datawave are used:

```bash
# Retrieve a JWT:
cd /path/to/datawave/services
export PKI_DIR=$PWD/spring-boot-starter-datawave/src/main/resources/pki
curl --cacert $PKI_DIR/ca.pem -E $PKI_DIR/user.pem https://localhost:8643/authorization/v1/authorize > /tmp/jwt.txt
curl -H "Authorization: Bearer $(</tmp/jwt.txt)" --cacert $PKI_DIR/ca.pem https://localhost:8843/dictionary/data/v1/
```

If you wish to run the audit service, it requires RabbitMQ. You must first
install and run RabbitMQ on your local host. If you have docker on Linux (on
a Mac you will have to map the ports through to the host rather than use host
networking), you can do this with:

```bash
docker run --rm -d --name rabbitmq --network=host rabbitmq:3.7-management-alpine
```

You should do this _before_ running any of the other services above, and when 
you launch each service (configuration, authorization, dictionary), remove 
`,nomessaging` from the activated profiles on the command-line for each. Then, you
can run the audit service as follows:

```bash
cd /path/to/datawave/services
java -jar audit-service/target/audit-service*-exec.jar --spring.profiles.active=dev 
```

Once the audit service is running, you can call it by passing the JWT you 
retrieved from the authorization service in an `Authorization` header. For example,
here is how you would check the health of the audit service:

```bash
curl -k -H "Authorization: Bearer <insert JWT text here>" https://localhost:8743/audit/mgmt/health
# Optional: save JWT in a text file, then pass it for future calls:
# curl -k <specify certificate info> https://localhost:8643/authorization/v1/authorize > /tmp/jwt.txt
# curl -q -k -H "Authorization: Bearer $(</tmp/jwt.txt)" https://localhost:8743/audit/mgmt/health
# NOTE: if you are using the supplied configuration, the test user cert will work:
# export PKI_DIR=spring-boot-starter-datawave/src/main/resources/pki
# curl --fail --cacert $PKI_DIR/ca.pem -E $PKI_DIR/user.pem https://localhost:8643/authorization/v1/authorize > /tmp/jwt.txt
# curl -q --fail --cacert $PKI_DIR/ca.pem -H "Authorization: Bearer $(</tmp/jwt.txt)" https://localhost:8743/audit/mgmt/health
```

You can send an audit request by posting to the `/v1/audit` endpoint. The following
example assumes you have saved your JWT provided by the authorization service in
`/tmp/jwt.txt`.

```bash
curl -q -k -H "Authorization: Bearer $(</tmp/jwt.txt)" \
--data-urlencode "auditUserDN=testUser" \
--data-urlencode "auditType=LOCALONLY" \
--data-urlencode "query=no query--testing the audit service" \
--data-urlencode "queryDate=20180101000000" \
--data-urlencode "auditColumnVisibility=USER" \
--data-urlencode "logicClass=EventQuery" \
--data-urlencode "auths=TEST" \
https://localhost:8743/audit/v1/audit
```

## Running With Consul

A production environment is likely to use DNS-based discovery provided by
something like Rancher or Kubernetes. However, if you wish to use Consul locally
for service discovery in development (note that it is also possible to simply
specify that all services are running on localhost), you must download Consul
from [here](https://www.consul.io/downloads.html). You can then run the agent
in dev mode on your local host:

```bash
cd /path/to/datawave/services
mkdir /tmp/consul.d
cat > /tmp/consul.d/consul.json <<_EOF_
{
    "datacenter": "demo_dc",
    "disable_update_check": true,
    "enable_agent_tls_for_checks": true,
    "key_file": "$PWD/common/src/main/resources/pki/server-key.pem",
    "cert_file": "$PWD/common/src/main/resources/pki/server-crt.pem",
    "ca_file": "$PWD/common/src/main/resources/pki/ca.pem"
}
_EOF_
consul agent -dev -ui -config-dir=/tmp/consul.d
```

Note that the `config.json` file tells Consul to provide the demo server X509
certificate when making health checks. If you have modified the sample
configuration to use a different certificate, then you should adjust the
configuration for your certificate and CA. Or, you can leave that step out
entirely (run without the `-config-dir=/tmp/consul.d` argument), everything
will work, but the health checks performed by Consul will fail on the
authorization service since it requires a client certificate. A third option
is to reconfigure the authorization service to not require a client certificate
(comment-out the server.ssl.client-auth property in `authorization.yml`).

Or, if you have Docker available just run the Consul image:

```bash
docker run -d --rm --name consul --network=host -v $PWD/common/src/main/resources/pki:/pki \
    -e CONSUL_LOCAL_CONFIG='{"datacenter": "demo_dc", \
        "disable_update_check": true, "enable_agent_tls_for_checks": true, \
        "key_file": "/pki/server-key.pem", "cert_file": "/pki/server-crt.pem", \
        "ca_file": "/pki/ca.pem"}' \
    consul:1.0.3
```

The Consul gui will then be available at http://localhost:8500/ui.

You will then need to enable the `consul` profile on each service you run by
adding `,consul` to the `--spring.profiles.active` list. Note that if you are
not using RabbitMQ and therefore are using the `nomessaging` profile, you will
need to list the `consul` profile before the `nomessaging` profile. E.g., to
run the authorization service with consul, but without RabbitMQ support, you
would run:

```bash
java -jar authorization-service/target/authorization-service*-exec.jar --spring.profiles.active=dev,consul,nomessaging
```

If you intend to run Consul and alo use RabbitMQ, then you must define a service
registration for RabbitMQ in Consul. You can do that by running Consul as follows
instead of the method described previously:

```bash
mkdir /tmp/consul.d
echo '{"service": {"name": "rabbitmq", "port": 5672}}' | tee /tmp/consul.d/rabbitmq.json
consul agent -dev -ui -config-dir=/tmp/consul.d
```

Or, if you have Docker, you can manually add a service definition to the running
Consul container:

```bash
docker exec -it consul sh -c 'echo '"'"'{"service": {"name": "rabbitmq", "port": 5672}}'"'"' | tee /consul/config/rabbitmq.json'
docker exec -it consul consul reload
```

## Running with a Hazelcast Server

A production environment will use Hazelcast for caching in the authorization
service. Hazelcast is set up to run in a client/server mode where the
authorization service runs a Hazelcast client which connect to a Hazelcast
server (which is  usually a cluster of servers). The dev profile template
disables this client, forcing the configuration to use an embedded Hazelcast
member that is only useful for testing. To test with the client/server form on
Hazelcast you will need to run one or more Hazelcast server processes (to form
a cluster) and configure the authorization service to run the Hazelcast client.

To tun the Hazelcast service, execute the following. Note that running the
Hazelcast service requires Consul.

```bash
java -jar hazelcast-service/target/hazelcast-server-*-exec.jar --spring.profiles.active=dev,consul,nomessaging
```

Remove the `,nomessaging` if you are using RabbitMQ. If you want to run more
than one copy of the service, you will need to specify a different secure and
non-secure port for each copy. For example, you could run a second and third
copy with:

```bash
java -jar hazelcast-service/target/hazelcast-server-*-exec.jar --spring.profiles.active=dev,consul,nomessaging --cachePort=8843
```

and

```bash
java -jar hazelcast-service/target/hazelcast-server-*-exec.jar --spring.profiles.active=dev,consul,nomessaging --cachePort=8943
```

To configure the authorization service to run the Hazelcast client, you can
comment-out or remove the following line from you `authorization-dev.yml` file:

```yml
hazelcast.client.enabled: ${hzClient:false}
```

Alternatively, you can set the value to `true`, or pass `--hzClient=true` on
the command-line.

## Building/Running with Docker

If you have Docker installed on your machine, then this demo can be built with
Docker images by enabling the docker maven profile:

```bash
mvn -Pdocker clean package
``` 

Note that you must override the os detection since you might not be building on
the same architecture that the Docker image will be running.

Then, using Docker Compose, you can run the demo:

```bash
cd docker
docker-compose up -d
```

You can watch logs with:

```bash
docker-compose logs -f
```
 
 And shut everything back down with:
 
 ```bash
 docker-compose down
 ```

The `docker-compose.yml` file launches the following services:

* Consul
* RabbitMQ
* Hazelcast Server
* Authorization - configured with a mock provider that returns canned credentials for any caller
* Audit

It will take a minute for the services to start and for the authorization
service to download its configuration from the configuration service. You can
run `docker ps` to find the port mappings for the various services. The
authorization service and auditing service get randomly assigned ports, which
allows scaling with `docker-compose scale`.

# Directory/Build Layout

The external services are organized as follows. Authorization, audit, and dictionary
have two components: api and service. For example, authorization-api contains the
components of the Authorization service that a client would use, and authorization-service
contains the implementation of the service. Each of these modules can be versioned
separately. If only code in authorization-service changes, its version can be
updated, and a new version of the authorization service deployed with no other
changes. If code in the authorization-api directory changes, then this is an API
change and care must be taken.

In addition to the common pattern for services, this directory also contains:
* `config-service`: The configuration service. It has no API (other than that 
  provided by Spring Boot), so there is no `config-api` module.
* `hazelcast-service`: This is the Hazelcast cache service. Multiple copies of this
  are intended to be run and form a cluster for caching data.
* `hazelcast-client`: This is the client code required to access a running
  Hazelcast clustered cache.
* `hazelcast-common`: This is code required by both `hazelcast-client` and `hazelcast-service`
* `metrics-reporter`: This contains code to publish metrics from a Dropwizard
  metrics registry to StatsD, NSQ, or Timely.
* `accumulo-utils`: General utilities for working with Accumulo and datawave marking functions
* `base-rest-responses`: Base rest response and exception classes that are used by query/service responses
* `common-utils`: Extremely low level utilities that might be used by any service (e.g., datawave-specific string utils)
* `type-utils`: Normalizers and data types that are part of the datawave metadata and query system
* `spring-boot-starter-datawave`: This is a service starter that can be used
  when creating a new microservice. See the "Common Service Starter" section above.
* `spring-boot-starter-datawave-cache`: This is a service starter that customizes the default
  spring boot caching layer. Since datawave services declare some named caches, this starter
  allows another cache configuration to be declared and marked as an @Primary bean in order
  to configure application-level caching
* `build-parent`: This contains a parent pom that references everything else in the
  services directory. Although each service/api can be versioned independently,
  it is desirable to be able to build everything together using one Maven command.
  This pom helps accomplish that goal.
* `docker-quickstart`: This contains quickstart configuration for running the
  microservices using Docker compose.
* `sample_configuration`: This contains example configuration that you might
  run the config service against in order to provide configuration for the various
  microservices.
