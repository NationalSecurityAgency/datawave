## Accumulo Service

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-accumulo-service/workflows/Tests/badge.svg)

The Accumulo service is an administrator utility that provides a rest API
which exposes a small subset of Accumulo's table and security operations to
external clients. Users must possess the **Administrator** role to access
the service's endpoints.

### Root Context

*https://host:port/accumulo/v1/*

---

### Admin API

| Method | Operation | Description | Request Body |
|:-------|:----------|:------------|:-------------|
| `POST` | admin/createTable/{tableName} | Creates the given table | N/A |
| `POST` | admin/flushTable/{tableName} | Initiates minor compaction on the given table | N/A |
| `POST` | admin/grantSystemPermission/{userName}/{permission} | Grants user the specified system permission | N/A |
| `POST` | admin/revokeSystemPermission/{userName}/{permission} | Revokes system permission from user | N/A |
| `POST` | admin/grantTablePermission/{userName}/{tableName}/{permission} | Grants user the specified table permission | N/A |
| `POST` | admin/revokeTablePermission/{userName}/{tableName}/{permission} | Revokes table permission from user | N/A |
| `POST` | admin/setTableProperty/{tableName}/{propertyName}/{propertyValue} | Sets the specified table property | N/A |
| `POST` | admin/removeTableProperty/{tableName}/{propertyName} | Removes the specified table property | N/A |
| `GET` | admin/listTables | Lists all Accumulo tables | N/A |
| `GET` | admin/listUsers | Lists all Accumulo users | N/A |
| `GET` | admin/listUserAuthorizations/{userName} | Lists the user's authorizations | N/A |
| `GET` | admin/listUserPermissions/{userName} | Lists the user's permissions | N/A |
| `PUT` | admin/update | Processes the specified table mutations | [UpdateRequest] |
| `POST` | admin/validateVisibilities | Validates one or more visibility expressions | *visibility* form param |

* See [AdminController] class for details

### Lookup API

Enables simple batch scans on a given table/row to retrieve results,
and also supports query auditing (i.e., when *audit-client.enabled:
true*, which is the default)  

| Method | Operation | Description | Request Body |
|:-------|:----------|:------------|:-------------|
| `GET`/`POST` | lookup/{tableName}/{row} | Scans the given row and returns scan results | Optional form params |

* Several optional parameters can be expressed via query string (GET) or
  form param (POST) 
   
* See [LookupController] class for details

### Stats API

May be used as a proxy for the Accumulo Monitor as a convenience, in order to expose
the monitor's raw stats response to external clients

| Method | Operation | Description | Request Body |
|:-------|:----------|:------------|:-------------|
| `GET` | stats | Retrieves the raw `stats` response from the Accumulo Monitor | N/A |

---

### Getting Started

1. First, refer to [services/README][getting-started] for launching the config,
   authorization, and audit services.

   * The authorization service should be launched with the `mock` profile to leverage
     test PKI materials and associated user configuration (see
     [authorization-mock.yml][auth-mock-yml]).

2. Launch this service as follows, with the `remoteauth` profile to enable client
   cert authentication, and with the `mock` profile to utilize an in-memory
   Accumulo instance containing some test data...
    
   ```
   java -jar service/target/accumulo-service*-exec.jar --spring.profiles.active=dev,remoteauth,mock
   ```

3. Ensure that the [testUser.p12][testUser] (password: *ChangeIt*) cert is imported into
   your browser, and then visit any of the following:

   * https://localhost:8943/accumulo/v1/lookup/warehouseTestTable/row1?colFam=cf1
   * https://localhost:8943/accumulo/v1/lookup/warehouseTestTable/row1?colFam=cf1&useAuthorizations=A,B,C
   * https://localhost:8943/accumulo/v1/lookup/warehouseTestTable/row1?colFam=cf1&useAuthorizations=A,D,E
   * https://localhost:8943/accumulo/v1/lookup/warehouseTestTable/row1?colFam=cf1&useAuthorizations=A,F,G
   * https://localhost:8943/accumulo/v1/admin/listTables
   * https://localhost:8943/accumulo/v1/admin/listUsers
   * https://localhost:8943/accumulo/v1/admin/listUserPermissions/root
   * Perform PUT and POST API operations with your preferred HTTP client, as desired
   
   *Note*: The `stats` API is not functional when using the `mock` profile, as it
   requires that ZooKeeper and Accumulo Monitor instances are running

   See [sample_configuration/accumulo-dev.yml][accumulo-dev-yml] and configure as desired

[getting-started]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/README.md#getting-started
[UpdateRequest]:api/src/main/java/datawave/webservice/request/UpdateRequest.java
[AdminController]:service/src/main/java/datawave/microservice/accumulo/admin/AdminController.java
[LookupController]:service/src/main/java/datawave/microservice/accumulo/lookup/LookupController.java
[StatsController]:service/src/main/java/datawave/microservice/accumulo/stats/StatsController.java
[testUser]:https://github.com/NationalSecurityAgency/datawave-spring-boot-starter/blob/master/src/main/resources/testUser.p12
[accumulo-dev-yml]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/sample_configuration/accumulo-dev.yml.example
[auth-mock-yml]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/sample_configuration/authorization-mock.yml

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0