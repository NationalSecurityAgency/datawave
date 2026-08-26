# ZkPojoPublisher

The interface [ZkPojoPublisher](ZkPojoPublisher.java) and its corresponding implementation [ZkPojoPublisherImpl](ZkPojoPublisherImpl.java) provides the ability to trigger and publish updates of a configured class instance to any subscribers using Zookeeper to listen for updates and triggering events. A publisher instance can be configured with the following:
* `zkClienttBuilder`: An instance of [ZkClientBuilder](ZkClientBuilder.java).
* `hdfsConfigUrls`: A comma-delimited list of hadoop configuration files.
* `pojoClass`: The class of the object type the publisher will deserialize and publish.

A ZkPojoPublisher will attempt to reload and publish a new instance of its configured class when one of the following happens:

* The node `/<namespace>/path` is created or modified with non-empty data.
* The node `/<namespace>/trigger` is created, modified, or deleted.

Upon receiving a trigger event, the publisher will attempt to read and deserialize an instance of the configured class from the filepath stored in the data of the node `/<namespace>/path`. The filepath is expected to point to an XML, JSON, or YAML file, and must conform to one of the following URI schemes:
* A URL: `http://path/to/file` or `https://path/to/file`
* An HDFS file: `hdfs://path/to/file`
* A local file: `file://path/to/file` or `/path/to/file`
 
If an instance of the class is successfully deserialized from the file, it will be validated against any configured object validators. Afterward it will be provided to all subscribers that have subscribed to the publisher via `ZkPojoPublisher.addListener(Consumer<T>)`. The status of any triggered attempt will be recorded under the node `/<namespace>/attempts/<serverIpAddress>/latest`, which will always reflect the latest reload attempt. The data of the node will be JSON serialized from an instance of [ZkPojoPublisherImpl.PublishAttempt](ZkPojoPublisherImpl.java):
```js
{
    // The epoch timestamp in ms when the publish atempt was triggered.
    "timestamp":1786412869000,
    // The root trigger for the publish attempt. 
    // See ZkPojoPublisherImpl.Trigger for possible values.
    "trigger":"PATH_NODE_MODIFIED",
    // The final status of the publish attempt.
    // See ZkPojoPublisherImpl.Status for possible values.
    "status":"SUCCESS",
    // A list of of any errors captured during the publish attempt, 
    // and any relevant stacktraces. 
    "errors": []    
    
}
```
An example of JSON where an error when trying to load the file occurred:
```js
{
    "timestamp":1786412869000,
    "trigger":"PATH_NODE_MODIFIED",
    "status":"LOAD_ERROR",
    "errors": [
        {
            "message": "File not found: i/do/not/exist",
            // Truncated here for the example, but the full stacktrace would be included in production.
            "stacktrace": "java.nio.file.NoSuchFileException: i/do/not/exist\n\tat ...."
        }
    ]    
    
}
```
An example of JSON where one or more listeners threw an error after being supplied the new POJO:
```js
{
    "timestamp":1786412869000,
    "trigger":"PATH_NODE_MODIFIED",
    "status":"LISTENER_ERROR",
    // and any relevant stacktraces. 
        "errors" : [ {
        "message" : "Exception thrown by listener datawave.zookeeper.ZkPojoPublisherImplTest$$Lambda$670/0x00000008403b5c40",
        // Truncated here for the example, but the full stacktrace would be included in production.
        "stacktrace" : "java.lang.NullPointerException: Something bad happened!\n\tat ...."
    }, {
        "message" : "Exception thrown by listener datawave.zookeeper.ZkPojoPublisherImplTest$$Lambda$671/0x00000008403b5040",
        // Truncated here for the example, but the full stacktrace would be included in production.
        "stacktrace" : "java.lang.IllegalArgumentException: I don't like this configuration.\n\tat ...."
    }, {
        "message" : "Exception thrown by listener datawave.zookeeper.ZkPojoPublisherImplTest$$Lambda$672/0x00000008403b5440",
        // Truncated here for the example, but the full stacktrace would be included in production.
        "stacktrace" : "java.lang.UnsupportedOperationException: Why do I even exist?\n\tat ...."
    } ]  
    
}
```
