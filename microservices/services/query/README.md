# Query Service

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave/workflows/Tests/badge.svg)

The query service is a user-facing DATAWAVE microservice that serves as the main REST interface for DataWave query functionality.  

### Query Context

*https://host:port/query/v1/*

### User API

| Done?   | New?    |Admin?   | Method        | Operation                                | Description                                                                                                | Path Param              | Request Body                   | Response Body                              |
|:--------|:--------|:--------|:--------------|:-----------------------------------------|:-----------------------------------------------------------------------------------------------------------|:------------------------|:-------------------------------|:-------------------------------------------|
| &check; |         |         | `GET`         | /listQueryLogic                          | List QueryLogic types that are currently available                                                         | N/A                     | N/A                            | [QueryLogicResponse]                       |
| &check; |         |         | `POST`        | /{queryLogic}/define                     | Define a query using the specified query logic and params                                                  | [QueryLogicName]        | [QueryParameters]              | [GenericResponse]                          |
| &check; |         |         | `POST`        | /{queryLogic}/create                     | Create a query using the specified query logic and params                                                  | [QueryLogicName]        | [QueryParameters]              | [GenericResponse]                          |
| &check; |         |         | `POST`        | /{queryLogic}/plan                       | Generate a query plan using the specified query logic and params                                           | [QueryLogicName]        | [QueryParameters]              | [GenericResponse]                          |
| &check; |         |         | `POST`        | /{queryLogic}/predict                    | Generate a query prediction using the specified query logic and params                                     | [QueryLogicName]        | [QueryParameters]              | [GenericResponse]                          |
| &check; |         |         | <s>`POST`</s> | <s>/{queryLogic}/async/create</s>        | <s>Create a query using the specified query logic and params</s>                                           | <s>[QueryLogicName]</s> | <s>[QueryParameters]</s>       | <s>[GenericResponse]</s>                   |
| &check; |         |         | `PUT` `POST`  | /{id}/reset                              | Resets the specified query                                                                                 | [QueryId]               | N/A                            | <s>[VoidResponse]</s><br>[GenericResponse] |
| &check; |         |         | `POST`        | /{queryLogic}/createAndNext              | Create a query using the specified query logic and params, and get the first page                          | [QueryLogicName]        | [QueryParameters]              | [BaseQueryResponse]                        |
| &check; |         |         | <s>`POST`</s> | <s>/{queryLogic}/async/createAndNext</s> | <s>Create a query using the specified query logic and params, and get the first page</s>                   | <s>[QueryLogicName]</s> | <s>[QueryParameters]</s>       | <s>[BaseQueryResponse]</s>                 |
|         |         |         | `GET`         | /lookupContentUUID/{uuidType}/{uuid}     | Returns content associated with the given UUID                                                             | [UUIDType], [UUID]      | N/A                            | [BaseQueryResponse] or [StreamingOutput]   |
|         |         |         | `POST`        | /lookupContentUUID                       | Returns content associated with the given batch of UUIDs                                                   | N/A                     | [QueryParameters]              | [BaseQueryResponse] or [StreamingOutput]   |
|         |         |         | `GET`         | /lookupUUID/{uuidType}/{uuid}            | Returns event associated with the given batch of UUID                                                      | [UUIDType], [UUID]      | N/A                            | [BaseQueryResponse] or [StreamingOutput]   |
|         |         |         | `POST`        | /lookupUUID                              | Returns event(s) associated with the given batch of UUIDs                                                  | N/A                     | [QueryParameters]              | [BaseQueryResponse] or [StreamingOutput]   |
| &check; |         |         | `GET`         | /{id}/plan                               | Returns the plan for the specified query                                                                   | [QueryId]               | N/A                            | [GenericResponse]                          |
| &check; |         |         | `GET`         | /{id}/predictions                        | Returns the predictions for the specified query                                                            | [QueryId]               | N/A                            | [GenericResponse]                          |
| &check; |         |         | <s>`GET`</s>  | <s>/{id}/async/next</s>                  | <s>Returns the next page of results for the specified query</s>                                            | <s>[QueryId]</s>        | <s>N/A</s>                     | <s>[BaseQueryResponse]</s>                 |
| &check; |         |         | `GET`         | /{id}/next                               | Returns the next page of results for the specified query                                                   | [QueryId]               | N/A                            | [BaseQueryResponse]                        |
| &check; |         |         | `PUT` `POST`  | /{id}/close                              | Closes the specified query                                                                                 | [QueryId]               | N/A                            | [VoidResponse]                             |
| &check; |         | &check; | `PUT` `POST`  | /{id}/adminClose                         | Closes the specified query                                                                                 | [QueryId]               | N/A                            | [VoidResponse]                             |
| &check; | &check; | &check; | `PUT` `POST`  | /adminCloseAll                           | Closes all running queries                                                                                 | N/A                     | N/A                            | [VoidResponse]                             |
| &check; |         |         | `PUT` `POST`  | /{id}/cancel                             | Cancels the specified query                                                                                | [QueryId]               | N/A                            | [VoidResponse]                             |
| &check; |         | &check; | `PUT` `POST`  | /{id}/adminCancel                        | Cancels the specified query                                                                                | [QueryId]               | N/A                            | [VoidResponse]                             |
| &check; | &check; | &check; | `PUT` `POST`  | /adminCancelAll                          | Cancels all running queries                                                                                | N/A                     | N/A                            | [VoidResponse]                             |
| &check; |         |         | <s>`GET`</s>  | <s>/listAll</s>                          | <s>Returns a list of queries associated with the current user</s>                                          | <s>N/A</s>              | <s>N/A</s>                     | <s>[QueryImplListResponse]</s>             |
| &check; |         |         | `GET`         | /{id}                                    | Returns query info for the specified query                                                                 | [QueryId]               | N/A                            | [QueryImplListResponse]                    |
| &check; |         |         | `GET`         | /list                                    | Returns a list of queries for this caller, filtering by the (optional) query id, and (optional) query name | N/A                     | [QueryId], [QueryName]         | [QueryImplListResponse]                    |
| &check; | &check; | &check; | `GET`         | /adminList                               | Returns a list of queries, filtered by the (optional) user, (optional) query id, and (optional) query name | N/A                     | [User], [QueryId], [QueryName] | [QueryImplListResponse]                    |
| &check; |         |         | `DELETE`      | /{id}/remove                             | Remove (delete) the specified query                                                                        | [QueryId]               | N/A                            | [VoidResponse]                             |
| &check; | &check; | &check; | `DELETE`      | /{id}/adminRemove                        | Remove (delete) the specified query                                                                        | [QueryId]               | N/A                            | [VoidResponse]                             |
| &check; | &check; | &check; | `DELETE`      | /{id}/adminRemoveAll                     | Removes all queries which aren't running                                                                   | N/A                     | N/A                            | [VoidResponse]                             |
| &check; |         |         | `POST`        | /{id}/duplicate                          | Duplicates the specified query                                                                             | [QueryId]               | [QueryParameters]              | [GenericResponse]                          |
| &check; |         |         | `PUT` `POST`  | /{id}/update                             | Updates the specified query                                                                                | [QueryId]               | [QueryParameters]              | [GenericResponse]                          |
| &check; |         |         | <s>`GET`</s>  | <s>/{id}/listAll</s>                     | <s>Returns a list of queries associated with the specified user</s>                                        | <s>[UserId]</s>         | <s>N/A</s>                     | <s>[QueryImplListResponse]</s>             |
| &check; |         |         | <s>`POST`</s> | <s>/purgeQueryCache</s>                  | <s>Purges the cache of query objects</s>                                                                   | <s>N/A</s>              | <s>N/A</s>                     | <s>[VoidResponse]</s>                      |
| &check; |         |         | <s>`GET`</s>  | <s>/enableTracing</s>                    | <s>Enables tracing for queries which match the given criteria</s>                                          | <s>N/A</s>              | <s>[QueryRegex], [User]</s>    | <s>[VoidResponse]</s>                      |
| &check; |         |         | <s>`GET`</s>  | <s>/disableTracing</s>                   | <s>Disables tracing for queries which match the given criteria</s>                                         | <s>N/A</s>              | <s>[QueryRegex], [User]</s>    | <s>[VoidResponse]</s>                      |
| &check; |         |         | <s>`GET`</s>  | <s>/disableAllTracing</s>                | <s>Disables tracing for all queries</s>                                                                    | <s>N/A</s>              | <s>N/A</s>                     | <s>[VoidResponse]</s>                      |
|         |         |         | `POST`        | /{logicName}/execute                     | Create a query using the specified query logic and params, and stream the results                          | [QueryLogicName]        | [QueryParameters]              | [StreamingOutput]                          |
|         |         |         | `POST`        | /{logicName}/async/execute               | Create a query using the specified query logic and params, and stream the results                          | [QueryLogicName]        | [QueryParameters]              | [StreamingOutput]                          |

---

### Getting Started

TBD

For now, refer to the [Datawave Docker Compose Readme][getting-started]

[getting-started]:https://github.com/NationalSecurityAgency/datawave/blob/feature/queryMicroservices/docker/README.md#datawave-docker-compose

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0
