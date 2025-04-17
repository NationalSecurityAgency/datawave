## Modification Service

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-modification-service/workflows/Tests/badge.svg)

The Dictionary service provides access to the modification api.  Based on the datawave metadata, certain
fields are modifiable (e.g. a document comment field) by users using this service.

### Root Context

*https://host:port/modification/*

---

### Modification

The root context for all modification operations is
*https://host:port/modification/v1/*

| Method | Operation     | Description                                       | Request Body    |
|:---    |:---           |:---                                               |:---             |
| `GET`  | /             | Retrieves the data dictionary                     | N/A             |
| `GET`  | /Descriptions | Retrieves all descriptions from the dictionary    | N/A             |
| `GET`  | /Descriptions/{datatype} | Retrieves all descriptions for a data type from the dictionary | N/A |
| `GET`  | /Descriptions/{datatype}/{fieldname} | Retrieves from the dictionary the description for a field of a data type | N/A |
| `POST` | /Descriptions | Uploads a set of descriptions into the dictionary | [DefaultFields] |
| `PUT`  | /Descriptions/{datatype}/{fieldName}/{description} | Sets the description for a field in a datatype | N/A |
| `POST` | /Descriptions | Sets the description for a field in a datatype    | N/A             |
| `DELETE`| /Descriptions/{datatype}/{fieldname} | Removes the description from a field of a data type | N/A |

* See [ModificationOperations] class for further details

---

### Getting Started

1. First, refer to [services/README](https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/README.md#getting-started) for launching the
   config, authorization, and audit services.

   * The authorization service should be launched with the `mock` profile to leverage
     test PKI materials and associated user configuration (see
     [authorization-mock.yml][auth-mock-yml]).

2. Launch this service as follows, with the `remoteauth` profile to enable client
   cert authentication.
    
   ```
   java -jar service/target/modification-service*-exec.jar --spring.profiles.active=dev,remoteauth
   ```

3. Ensure that the [testUser.p12][testUser] (password: *ChangeIt*) cert is imported into
   your browser, and then visit any of the following:

   * https://localhost:8843/modification/v1/
   * Perform PUT and POST API operations with your preferred HTTP client, as desired
   
   See [sample_configuration/modification-dev.yml][modification-dev-yml] and configure as desired


[ModificationOperations]:service/src/main/java/datawave/microservice/modification/ModificationOperations.java
[testUser]:https://github.com/NationalSecurityAgency/datawave-spring-boot-starter/blob/master/src/main/resources/testUser.p12
[modification-dev-yml]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/sample_configuration/modification-dev.yml.example

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0
