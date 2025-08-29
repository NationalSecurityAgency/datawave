## Dictionary Service

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-dictionary-service/workflows/Tests/badge.svg)

The Dictionary service provides access to the data dictionary and edge
dictionary which provide metadata about fields that are stored
in Accumulo. The Dictionary service also supports manipulation of models which are 
contained in the data dictionary table.

### Root Context

*https://host:port/dictionary/*

---

### Data Dictionary

The root context for all data dictionary operations is:
* V1: *https://host:port/dictionary/data/v1/*
* V2: *https://host:port/dictionary/data/v2/*

| Method   | Operation                                          | Description                                                                                               | Request Body     | Version |
|:---------|:---------------------------------------------------|:----------------------------------------------------------------------------------------------------------|:-----------------|:--------|
| `GET`    | /                                                  | Retrieves the data dictionary                                                                             | N/A              | V1, V2  |
| `GET`    | /Descriptions                                      | Retrieves all descriptions from the dictionary                                                            | N/A              | V1, V2  |
| `POST`   | /Descriptions                                      | Uploads a set of descriptions into the dictionary                                                         | [DefaultFields]  | V1, V2  |
| `POST`   | /Descriptions                                      | <strong>(Administrator credentials required)</strong> Sets the description for a field in a datatype      | N/A              | V1, V2  |
| `GET`    | /Descriptions/{datatype}                           | Retrieves all descriptions for a data type from the dictionary                                            | N/A              | V1, V2  |
| `GET`    | /Descriptions/{datatype}/{fieldname}               | Retrieves from the dictionary the description for a field of a data type                                  | N/A              | V1, V2  |
| `DELETE` | /Descriptions/{datatype}/{fieldname}               | <strong>(Administrator credentials required)</strong> Removes the description from a field of a data type | N/A              | V1, V2  |
| `PUT`    | /Descriptions/{datatype}/{fieldName}/{description} | <strong>(Administrator credentials required)</strong> Sets the description for a field in a datatype      | N/A              | V1, V2  |

* See [DataDictionaryControllerV1] and [DataDictionaryControllerV2] for further details

### Edge Dictionary

The root context for all edge dictionary operations is:
* *https://host:port/dictionary/edge/v1/*

| Method | Operation | Description                   | Request Body |
|:---    |:---       |:------------------------------|:---          |
| `GET`  | /         | Retrieves the edge dictionary | N/A          |

* See [EdgeDictionaryController] for further details

### Models
The root context for all model operations is:
* *https://host:port/dictionary/model/v1/*

| Method | Operation | Description                                                                                             | Request Body |
|:-------|:----------|:--------------------------------------------------------------------------------------------------------|:-------------|
| `GET`  | /list     | Retrieves the names of the models                                                                       | N/A          |
| `GET`  | /{name}   | <strong>(Administrator credentials required)</strong> Delete a model with the supplied name             | N/A          |
| `GET`  | /clone    | <strong>(Administrator credentials required)</strong> Copy a model                                      | N/A          |
| `GET`  | /{name}   | Retrieve the model and all of its mappings.                                                             | N/A          |
| `GET`  | /insert   | <strong>(Administrator credentials required)</strong> Insert a new field mapping into an existing model | [Model]      |
| `GET`  | /import   | <strong>(Administrator credentials required)</strong> Insert a new field mapping into an existing model | [Model]      |
| `GET`  | /delete   | <strong>(Administrator credentials required)</strong> Delete field mappings from an existing model      | [Model]      |
* See [ModelController] for further details
---

### Getting Started

#### Using DATAWAVE Docker Compose 

* You can go [here] for more information related to building and starting the dictionary service (as well as other services).

#### Using an Alternate Way
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


[DataDictionaryControllerV1]:service/src/main/java/datawave/microservice/dictionary/DataDictionaryControllerV1.java
[DataDictionaryControllerV2]:service/src/main/java/datawave/microservice/dictionary/DataDictionaryControllerV2.java
[EdgeDictionaryController]:service/src/main/java/datawave/microservice/dictionary/EdgeDictionaryController.java
[ModelController]:service/src/main/java/datawave/microservice/model/ModelController.java
[DefaultFields]:api/src/main/java/datawave/webservice/dictionary/data/DefaultFields.java
[Model]:api/src/main/java/datawave/webservice/model/Model.java
[testUser]:https://github.com/NationalSecurityAgency/datawave-spring-boot-starter/blob/master/src/main/resources/testUser.p12
[dictionary-dev-yml]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/sample_configuration/dictionary-dev.yml.example
[here]: https://github.com/NationalSecurityAgency/datawave/blob/integration/docker/README.md#datawave-docker-compose
[auth-mock-yml]: https://github.com/NationalSecurityAgency/datawave/blob/integration/docker/config/authorization-mock.yml
[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0