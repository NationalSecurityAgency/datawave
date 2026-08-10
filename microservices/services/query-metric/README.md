## Query Metric Service

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-query-metric-service/workflows/Tests/badge.svg)

The Query Metric service allows query services to store and update query metrics.
Callers must have the Administrator role to call either updateMetric operations.
When running multiple service instances and using a Hazelcast cache, successive metric updates 
can be made on any of the service instances.

Any user can call the id/{queryId} operation, but users who do not have the metric admin role
can only retrieve a query metric for a query that they ran.

### Query Metric API V1

*https://host:port/querymetric/v1/*

| Method | Operation     | Description                           | Request Body                |
|:---    |:---           |:---                                   |:---                         |
| `POST` | updateMetric  | Update a single BaseQueryMetric       | BaseQueryMetric             |
| `POST` | updateMetrics | Update a list of BaseQueryMetric      | List&lt;BaseQueryMetric&gt; |
| `GET`  | id/{queryId}  | Retrieve a BaseQueryMetric by queryId | N/A                         |

* See [QueryMetricOperations](src/main/java/datawave/microservice/querymetric/QueryMetricOperations.java)
  class for details

---
### Design

The Query Metric service is designed so that metric updates can be sent to any running instance of  
the service.  This is accomplished by using two Hazelcast distributed map data structures.

- incomingQueryMetrics - A write-behind String-BaseQueryMetric map where metrics are placed as
soon as they are received.  When a new metric is received, this map is llocked and checked for
an existing entry.  The new metric is combined with any existing entry (PageMetrics can get lengthy and
are often truncated) and then placed in the map.  The write-behind feature allows several quick updates 
to take place for each Accumulo write.  A configured MapStore is periodically called on one of the service 
instances to write updated metrics to Accumulo.  It can also retrieve a metric from Accumulo that is asked for
but missing from the map.
  
- lastWrittenQueryMetrics - A read-through String-BaseQueryMetric map that is used by incomingQueryMetrics's 
MapStore to determine which keys in Accumulo need to be deleted when a new metric is written to Accumulo. As a 
read-through map, it can retrieve a metric from Accumulo that is asked for but missing from the map.

---

### Getting Started

1. First, refer to [services/README](https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/README.md#getting-started)
   for launching the config service.

2. Launch this service as follows:

   ```
   java -jar target/query-metric-service*-exec.jar --spring.profiles.active=dev
   ```

   See [sample_configuration/querymetric-dev.yml.example](https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/sample_configuration/querymetric-dev.yml.example) 
   and configure as desired.

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0
