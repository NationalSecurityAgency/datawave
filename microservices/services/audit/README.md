# Audit Service

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-audit-service/workflows/Tests/badge.svg)

The audit service is a DATAWAVE microservice that provides
query audit capabilities.

### Audit Context

*https://host:port/audit/v1/*

### User API

| Method | Operation | Description            | Path Param | Request Body   |
|:-------|:----------|:-----------------------|:-----------|:---------------|
| `POST` | /audit    | Sends an audit request | N/A        | [AuditRequest] |

---

### Replay Context

*https://host:port/audit/v1/replay/*

### Replay API

| Method   | Operation       | Description                                    | Path Param | Request Body    |
|:---------|:----------------|:-----------------------------------------------|:-----------|:----------------|
| `POST`   | /create         | Creates an audit replay request                | N/A        | [ReplayRequest] |
| `POST`   | /createAndStart | Creates an audit replay request, and starts it | N/A        | [ReplayRequest] |
| `PUT`    | /{id}/start     | Starts an audit replay                         | [ReplayId] | N/A             |
| `GET`    | /{id}/status    | Gets the status of an audit replay             | [ReplayId] | N/A             |
| `PUT`    | /{id}/update    | Updates an audit replay                        | [ReplayId] | [SendRate]      |
| `PUT`    | /{id}/stop      | Stops an audit replay                          | [ReplayId] | N/A             |
| `PUT`    | /{id}/resume    | Resumes an audit replay                        | [ReplayId] | N/A             |
| `DELETE` | /{id}/delete    | Deletes an audit replay                        | [ReplayId] | N/A             |
| `PUT`    | /startAll       | Starts all audit replays                       | N/A        | N/A             |
| `GET`    | /statusAll      | Gets the status for all audit replays          | N/A        | N/A             |
| `PUT`    | /updateAll      | Updates all audit replays                      | N/A        | [SendRate]      |
| `PUT`    | /stopAll        | Stops all audit replays                        | N/A        | N/A             |
| `PUT`    | /resumeAll      | Resumes all audit replays                      | N/A        | N/A             |
| `DELETE` | /deleteAll      | Deletes all audit replays                      | N/A        | N/A             |

---

### Getting Started

1. First, refer to [services/README][getting-started] for launching the config
   and authorization services.

   * The authorization service should be launched with the `mock` profile to leverage
     test PKI materials and associated user configuration (see
     [authorization-mock.yml][auth-mock-yml]).

2. Launch this service as follows, with the `remoteauth` profile to enable client
   cert authentication...
    
   ```
   java -jar service/target/audit-service*-exec.jar --spring.profiles.active=dev,remoteauth,mock
   ```

3. Ensure that the [PKI Dir][pki-dir] is checked out locally somewhere, and set
   in the environment variable `PKI_DIR`.

4. Submit an audit request using curl.
   ```bash
   curl -q -k --cacert $PKI_DIR/ca.pem -E $PKI_DIR/user.pem \
   --data-urlencode "auditUserDN=testUser" \
   --data-urlencode "auditType=LOCALONLY" \
   --data-urlencode "query=no query--testing the audit service" \
   --data-urlencode "queryDate=1514764800000" \
   --data-urlencode "auditColumnVisibility=USER" \
   --data-urlencode "logicClass=EventQuery" \
   --data-urlencode "auths=TEST" \
   https://localhost:8743/audit/v1/audit
   ```

   See [sample_configuration/audit-dev.yml][audit-dev-yml] and configure as desired

[getting-started]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/README.md#getting-started
[AuditParameters]:api/src/main/java/datawave/webservice/common/audit/AuditParameters.java
[pki-dir]:https://github.com/NationalSecurityAgency/datawave-spring-boot-starter/blob/master/src/main/resources/pki
[audit-dev-yml]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/sample_configuration/audit-dev.yml.example
[auth-mock-yml]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/sample_configuration/authorization-mock.yml

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0
