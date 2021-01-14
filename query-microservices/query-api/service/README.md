# Query API Service

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave/workflows/Tests/badge.svg)

The query api service is a DATAWAVE microservice that provides
query capabilities.

### Query API Context

*https://host:port/query-api/v1/*

### User API

| Method | Operation | Description            | Path Param | Request Body   | Response Body |
|:-------|:----------|:-----------------------|:-----------|:---------------|
| `GET`         | /listQueryLogic                      | List QueryLogic types that are currently available                                | N/A                | N/A                  | [QueryLogicResponse]                     |
| `POST`        | /{queryLogic}/define                 | Define a query using the specified query logic and params                         | [QueryLogicName]   | [QueryParameters]    | [GenericResponse]                        |
| `POST`        | /{queryLogic}/create                 | Create a query using the specified query logic and params                         | [QueryLogicName]   | [QueryParameters]    | [GenericResponse]                        |
| `POST`        | /{queryLogic}/plan                   | Generate a query plan using the specified query logic and params                  | [QueryLogicName]   | [QueryParameters]    | [GenericResponse]                        |
| `POST`        | /{queryLogic}/predict                | Generate a query prediction using the specified query logic and params            | [QueryLogicName]   | [QueryParameters]    | [GenericResponse]                        |
| `POST`        | /{queryLogic}/async/create           | Create a query using the specified query logic and params                         | [QueryLogicName]   | [QueryParameters]    | [GenericResponse]                        |
| `PUT`         | /{id}/reset                          | Resets the specified query                                                        | [QueryId]          | N/A                  | [VoidResponse]                           |
| `POST`        | /{queryLogic}/createAndNext          | Create a query using the specified query logic and params, and get the first page | [QueryLogicName]   | [QueryParameters]    | [BaseQueryResponse]                      |
| `POST`        | /{queryLogic}/async/createAndNext    | Create a query using the specified query logic and params, and get the first page | [QueryLogicName]   | [QueryParameters]    | [BaseQueryResponse]                      |
| `GET`         | /lookupContentUUID/{uuidType}/{uuid} | Returns content associated with the given UUID                                    | [UUIDType], [UUID] | N/A                  | [BaseQueryResponse] || [StreamingOutput] |
| `POST`        | /lookupContentUUID                   | Returns content associated with the given batch of UUIDs                          | N/A                | [QueryParameters]    | [BaseQueryResponse] || [StreamingOutput] |
| `GET`         | /lookupUUID/{uuidType}/{uuid}        | Returns event associated with the given batch of UUID                             | [UUIDType], [UUID] | N/A                  | [BaseQueryResponse] || [StreamingOutput] |
| `POST`        | /lookupUUID                          | Returns event(s) associated with the given batch of UUIDs                         | N/A                | [QueryParameters]    | [BaseQueryResponse] || [StreamingOutput] |
| `GET`         | /{id}/plan                           | Returns the plan for the specified query                                          | [QueryId]          | N/A                  | [GenericResponse]                        |
| `GET`         | /{id}/predictions                    | Returns the predictions for the specified query                                   | [QueryId]          | N/A                  | [GenericResponse]                        |
| `GET`         | /{id}/async/next                     | Returns the next page of results for the specified query                          | [QueryId]          | N/A                  | [BaseQueryResponse]                      |
| `GET`         | /{id}/next                           | Returns the next page of results for the specified query                          | [QueryId]          | N/A                  | [BaseQueryResponse]                      |
| `PUT` `POST`  | /{id}/close                          | Closes the specified query                                                        | [QueryId]          | N/A                  | [VoidResponse]                           |
| `PUT` `POST`  | /{id}/adminClose                     | Closes the specified query                                                        | [QueryId]          | N/A                  | [VoidResponse]                           |
| `PUT` `POST`  | /{id}/cancel                         | Cancels the specified query                                                       | [QueryId]          | N/A                  | [VoidResponse]                           |
| `PUT` `POST`  | /{id}/adminCancel                    | Cancels the specified query                                                       | [QueryId]          | N/A                  | [VoidResponse]                           |
| `GET`         | /listAll                             | Returns a list of queries associated with the current user                        | N/A                | N/A                  | [QueryImplListResponse]                  |
| `GET`         | /{id}                                | Returns query info for the specified query                                        | [QueryId]          | N/A                  | [QueryImplListResponse]                  |
| `GET`         | /list                                | Returns a list of queries associated with the current user with given name        | N/A                | [Name]               | [QueryImplListResponse]                  |
| `DELETE`      | /{id}/remove                         | Remove (delete) the specified query                                               | [QueryId]          | N/A                  | [VoidResponse]                           |
| `POST`        | /{id}/duplicate                      | Duplicates the specified query                                                    | [QueryId]          | [QueryParameters]    | [GenericResponse]                        |
| `PUT` `POST`  | /{id}/update                         | Updates the specified query                                                       | [QueryId]          | [QueryParameters]    | [GenericResponse]                        |
| `GET`         | /{id}/listAll                        | Returns a list of queries associated with the specified user                      | [UserId]           | N/A                  | [QueryImplListResponse]                  |
| `POST`        | /purgeQueryCache                     | Purges the cache of query objects                                                 | N/A                | N/A                  | [VoidResponse]                           |
| `GET`         | /enableTracing                       | Enables tracing for queries which match the given criteria                        | N/A                | [QueryRegex], [User] | [VoidResponse]                           |
| `GET`         | /disableTracing                      | Disables tracing for queries which match the given criteria                       | N/A                | [QueryRegex], [User] | [VoidResponse]                           |
| `GET`         | /disableAllTracing                   | Disables tracing for all queries                                                  | N/A                | N/A                  | [VoidResponse]                           |
| `POST`        | /{logicName}/execute                 | Create a query using the specified query logic and params, and stream the results | [QueryLogicName]   | [QueryParameters]    | [StreamingOutput]                        |
| `POST`        | /{logicName}/async/execute           | Create a query using the specified query logic and params, and stream the results | [QueryLogicName]   | [QueryParameters]    | [StreamingOutput]                        |

---

### Getting Started

TBD

[getting-started]:https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/README.md#getting-started
[pki-dir]:https://github.com/NationalSecurityAgency/datawave-spring-boot-starter/blob/master/src/main/resources/pki

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0