## Dictionary Service

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-dictionary-service/workflows/Tests/badge.svg)

The Dictionary service provides access to the data dictionary and edge
dictionary. These services provide metadata about fields that are stored
in Accumulo.

### Root Context

*https://host:port/dictionary/*

---

### Data Dictionary

The root context for all data dictionary operations is
*https://host:port/dictionary/data/v1/*

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

* See [DataDictionaryOperations] class for further details

### Edge Dictionary

The root context for all edge dictionary operations is
*https://host:port/dictionary/edge/v1/*

| Method | Operation | Description                   | Request Body |
|:---    |:---       |:---                           |:---          |
| `GET`  | /         | Retrieves the edge dictionery | N/A          |

* See [EdgeDictionaryOperations] class for further details

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
   java -jar service/target/dictionary-service*-exec.jar --spring.profiles.active=dev,remoteauth
   ```

3. Ensure that the [testUser.p12][testUser] (password: *ChangeIt*) cert is imported into
   your browser, and then visit any of the following:

   * https://localhost:8843/dictionary/data/v1/
   * https://localhost:8843/dictionary/edge/v1/
   * Perform PUT and POST API operations with your preferred HTTP client, as desired
   
   See [sample_configuration/dictionary-dev.yml][dictionary-dev-yml] and configure as desired


[DataDictionaryOperations]:service/src/main/java/datawave/microservice/dictionary/DataDictionaryOperations.java
[EdgeDictionaryOperations]:service/src/main/java/datawave/microservice/dictionary/EdgeDictionaryOperations.java
[DefaultFields]:api/src/main/java/datawave/webservice/results/datadictionary/DefaultFields.java
[testUser]:https://github.com/NationalSecurityAgency/datawave-spring-boot-starter/blob/master/src/main/resources/testUser.p12
[dictionary-dev-yml]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/sample_configuration/dictionary-dev.yml.example

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0