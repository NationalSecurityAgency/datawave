## Authorization Service

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-authorization-service/workflows/Tests/badge.svg)

The Authorization service provides basic authorization for all other datawave
microservices. Authorization is a single endpoint that returns a signed
JSON Web Token (JWT) that represents a list of [DatawaveUser](api/src/main/java/datawave/security/authorization/DatawaveUser.java)
objects. Authorization may be performed by a trusted entity (i.e., a server) on
behalf of another user or chain of servers leading to a user.

The Authorization service caches authorized users and also provides an
administrative rest API to query and manage the cache.

When the fields 'email' and 'login' were added to DatawaveUser, we created V2 of the authorization and oauth API methods
to ensure that microservices using the pre-change authorization-api could still deserialize the JSON or JWT serialized DatawaveUser
when they call the V1 methods.  

### Authorization API V1

*https://host:port/authorization/v1/*

| Method | Operation | Description                            | Request Body |
|:---    |:---       |:---                                    |:---          |
| `GET`  | authorize | Authorizes the calling user            | N/A          |
| `GET`  | whoami    | Returns details about the calling user | N/A          |


### Authorization API V2

*https://host:port/authorization/v2/*

| Method | Operation | Description                            | Request Body |
|:---    |:---       |:---                                    |:---          |
| `GET`  | authorize | Authorizes the calling user            | N/A          |
| `GET`  | whoami    | Returns details about the calling user | N/A          |

### OAuth API V2

*https://host:port/authorization/v2/oauth/*

| Method | Operation | Description                            | Request Body |
|:---    |:---       |:---                                    |:---          |
| `GET`  | authorize | For registered client_id and authorized user, return a short-lived code that can be used by the client to retrieve a user's JWT  | N/A  |
| `POST` | token     | Using either a code from 'authorize' or a refresh_token, a registered can fetch the corresponding user's JWT                     | N/A  |
| `GET`  | user      | Returns details about primary current (by token or PKI) user                                                                     | N/A  |
| `GET`  | users     | Returns details about all current (by token or PKI) proxied users                                                                | N/A  |


### Admin API

Users must possess the **Administrator** role to access any of the admin methods.

| Method   | Operation                | Description                             | Request Body |
|:---      |:---                      |:---                                     |:---          |
| `DELETE` | admin/evictAll           | Deletes all users from the cache        | N/A          |
| `DELETE` | admin/evictUser          | Deletes the named user from the cache   | N/A          |
| `DELETE` | admin/evictUsersMatching | Deletes users with names containing the supplied string from the authorization cache | N/A |
| `GET`    | admin/listUsers          | Shows all users in the cache            | N/A          |
| `GET`    | admin/listUser           | Retrieves the named user from the cache | N/A          |
| `GET`    | admin/listUsersMatching  | Retrieves users with names containing the supplied string from the authorization cache | N/A |

* See [AuthorizationOperations](service/src/main/java/datawave/microservice/authorization/AuthorizationOperations.java)
  class for details

---

### Getting Started

1. First, refer to [services/README](https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/README.md#getting-started)
   for launching the config service.

2. Launch this service as follows, with the `mock` profile to leverage test PKI
   materials and associated user configuration (see [authorization-mock.yml][auth-mock-yml]).
    
   ```
   java -jar service/target/authorization-service*-exec.jar --spring.profiles.active=dev,mock
   ```

3. Ensure that the [testUser.p12][testUser] (password: *ChangeIt*) cert is
   imported into your browser, and then visit any of the following:

   * https://localhost:8643/authorization/v1/authorize
   * https://localhost:8643/authorization/v1/whoami
   * https://localhost:8643/authorization/v1/admin/listAll
   * https://localhost:8643/authorization/v1/admin/listUser?username=test
   * https://localhost:8643/authorization/v1/admin/listUsersMatching?username=test
   * Perform PUT and POST API operations with your preferred HTTP client, as desired
   
   See [sample_configuration/authorization-dev.yml][authorization-dev-yml] and configure as desired

[auth-mock-yml]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/sample_configuration/authorization-mock.yml
[testUser]:https://github.com/NationalSecurityAgency/datawave-spring-boot-starter/blob/master/src/main/resources/testUser.p12
[authorization-dev-yml]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/sample_configuration/authorization-dev.yml.example

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0
