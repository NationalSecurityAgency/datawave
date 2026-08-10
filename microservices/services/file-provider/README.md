# datawave-file-provider-service

### Getting Started With Docker
Refer to the [docker/README](https://github.com/NationalSecurityAgency/datawave/blob/integration/docker/README.md)

### Getting Started Without Docker
1. First, refer to [services/README](https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/README.md#getting-started)
   for launching the config service.

2. Launch this service as follows

   ```
   java -jar service/target/file-provider-service*-exec.jar --spring.profiles.active=dev
   ```