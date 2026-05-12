# ZkObjectPublisher

The class [ZkObjectPublisher](ZkObjectPublisher.java) provides the ability to trigger and publish updates of a configured class instance to any subscribers using Zookeeper to listen for updates and triggering events. A publisher instance can be configured with the following:
* `namespace`: The unique namespace for the ZkObjectPublisher. It is critical that this namespace is unique to any configured ZkObjectPublisher instances on the same server in order to prevent multiple publishers from writing to the same `/<namespace>/attempts/<serverIpAddress>` node in Zookeeper.
* `zookeeperConfig`: The zookeeper connect string, or a filepath of a zookeeper configuration file.
* `hdfsConfigUrls`: A comma-delimited list of hadoop configuration files.
* `objectClass`: The class of the object type the publisher will deserialize and publish.
* `objectValidators`: All validators that a successfully deserialized instance of `objectClass` will be supplied to before being supplied to all subscribers.

A ZkObjectPublisher will attempt to reload and publish a new instance of its configured class when one of the following happens:

* The node `/<namespace>/path` is created or modified with non-empty data.
* The node `/<namespace>/trigger` is created, modified, or deleted.

Upon receiving a trigger event, the publisher will attempt to read and deserialize an instance of the configured class from the filepath stored in the data of the node `/<namespace>/path`. The filepath is expected to be XML, JSON, or YAML, and must conform to one of the following URI schemes:
* A URL: `http://path/to/file` or `https://path/to/file`
* An HDFS file: `hdfs://path/to/file`
* A local file: `file://path/to/file` or `/path/to/file`
 
If an instance of the class is successfully deserialized from the file, it will be validated against any configured object validators. Afterward it will be provided to all subscribers that have subscribed to the publisher via `ZkObjectPublisher.subscribeToUpdates(Consumer)`. The status of any triggered attempt will be recorded under the node `/<namespace>/attempts/<serverIpAddress>`. Upon a success, the children of that node will follow the structure:

```text
/status # The data will be SUCCESS
/cause  # The data will be one of the values of the enum ZkObjectPublishCause
/time   # The data will be an ISO-8601 string representing the time of the publish attempt
```
If an error occurs, either when loading an instance of the class from the file, or when providing the new instance to subscribers, the children will follow the structure:
```text
/status                     # The data will be RELOAD_ERROR or SUBSCRIBER_ERROR
/cause                      # The data will be one of the values of the enum ZkObjectPublishCause
/time                       # The data will be an ISO-8601 string representing the time of the publish attempt
/errors                     # A node containing error_N nodes where N is a number ranging from 0 to one less than the total errors
/errors/error_N/message     # A short description of the error
/errors/error_N/stacktrace  # The stack trace of the error's exception, if any. If no exception was caught, this node will not exist.
```
The nodes under `/<namespace>/attempts/<serverIpAddress>` will always reflect the latest reload attempt.
