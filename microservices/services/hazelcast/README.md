# Hazelcast Service

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-hazelcast-service/workflows/Tests/badge.svg)

The Hazelcast service is an implementation of a distributed cache using
[Hazelcast In-Memory Data Grid](https://hazelcast.com/products/imdg/).
This service doesn't present a user-accessible endpoint, but rather is
intended for use in a microservices environment where other services are
the clients.

### Getting Started

Build the service with:
```bash
mvn -Pexec clean package
# Optional: use -Pdocker instead of -Pexec to build a docker image
```

**NOTE:** The Hazelcast service uses Spring Cloud service discovery to discover
service instances and form a clustered cache. Therefore, either the `k8s` or
`consul` profile must be enabled to support service discovery. For local
testing, it is easiest to use [consul](https://www.consul.io). These
instructions


1. First, refer to [services/README][getting-started] for launching the config service.

2. Ensure that the [PKI Dir][pki-dir] is checked out locally somewhere, and set
   in the environment variable `PKI_DIR`.

3. Launch consul:
    ```bash
    mkdir /tmp/consul.d
    cat > /tmp/consul.d/consul.json <<_EOF_
    {
        "datacenter": "demo_dc",
        "disable_update_check": true,
        "enable_agent_tls_for_checks": true,
        "key_file": "$PWD/datawave-spring-boot-starter/src/main/resources/pki/server-key.pem",
        "cert_file": "$PWD/datawave-spring-boot-starter/src/main/resources/pki/server-crt.pem",
        "ca_file": "$PWD/datawave-spring-boot-starter/src/main/resources/pki/ca.pem"
    }
    _EOF_
    consul agent -dev -ui -config-dir=/tmp/consul.d
    ```
    Or, if you have docker, use the following:
    ```bash
    docker run -d --rm --name consul --network=host -v $PWD/datawave-spring-boot-starter/src/main/resources/pki:/pki \
        -e CONSUL_LOCAL_CONFIG='{"datacenter": "demo_dc", \
            "disable_update_check": true, "enable_agent_tls_for_checks": true, \
            "key_file": "/pki/server-key.pem", "cert_file": "/pki/server-crt.pem", \
            "ca_file": "/pki/ca.pem"}' \
        consul:1.0.3
    ```

4. Launch this service as follows, with the `consul` profile to enable consul-based service discovery.
    
   ```
   java -jar service/target/hazelcast-service-*-exec.jar --spring.profiles.active=dev,consul,nomessaging
   ```

5. Optionally run two more copies to reach a three node Hazelcast cluster:
   ```bash
   java -jar service/target/hazelcast-service-*-exec.jar --spring.profiles.active=dev,consul,nomessaging --cachePort=8843
   java -jar service/target/hazelcast-service-*-exec.jar --spring.profiles.active=dev,consul,nomessaging --cachePort=8943
   ```

See [sample_configuration/cache-dev.yml][cache-dev-yml] and configure as desired

The hazelcast server on its own provides no rest interface. Instead it is
intended to be used with a Hazelcast client. The client source code can be
found in the [client](client) module. It acts as a Spring Boot cache provider
and can be used as a normal Spring Boot cache.

[getting-started]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/README.md#getting-started
[pki-dir]:https://github.com/NationalSecurityAgency/datawave-spring-boot-starter/blob/master/src/main/resources/pki
[cache-dev-yml]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/sample_configuration/cache-dev.yml.example

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0