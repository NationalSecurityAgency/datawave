package datawave.zookeeper;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.zookeeper.data.Stat;

import com.ctc.wstx.stax.WstxInputFactory;
import com.ctc.wstx.stax.WstxOutputFactory;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.format.DataFormatDetector;
import com.fasterxml.jackson.core.format.DataFormatMatcher;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.base.Preconditions;

import datawave.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A publisher that can be triggered to deserialize and publish updates of a configured class to subscribers. The publisher leverages Zookeeper and is triggered
 * by changes to Zookeeper nodes. The publisher will be triggered to reload an instance of the configured object when:
 * <ul>
 * <li>The node {@code /<namespace>/path} is created or modified with non-empty data.</li>
 * <li>The node {@code /<namespace>/trigger} is created, modified, or deleted.</li>
 * </ul>
 * Upon receiving a trigger event, the publisher will attempt to read and deserialize an instance of the configured class from the filepath stored in the data
 * of the node {@code /<namespace>/path}. The filepath is expected to be XML, JSON, or YAML, and must conform to one of the following URI schemes:
 * <ul>
 * <li>A URL: {@code http://path/to/file} or {@code https://path/to/file}.</li>
 * <li>An HDFS file: {@code hdfs://path/to/file}.</li>
 * <li>A local file: {@code file://path/to/file} or {@code /path/to/file}.</li>
 * </ul>
 * If an instance of the class is successfully deserialized from the file, it will be validated against any configured object validators. Afterward it will be
 * provided to all subscribers that have subscribed to the publisher via {@link ZkPojoPublisherImpl#subscribeToUpdates(Consumer)}. The status of any triggered
 * attempt will be recorded under the node {@code /<namespace>/attempts/<serverIpAddress>}. Upon a success, the children nodes will follow the structure:
 *
 * <pre>
 * /status # The data will be {@link PublishStatus#SUCCESS}
 * /cause  # The data will be one of {@link PublishCause}
 * /time   # The data will be an ISO-8601 string representing the time of the publish attempt
 * </pre>
 *
 * If an error occurs, either when loading an instance of the class from the file, or when providing the new instance to subscribers, the children will follow
 * the structure:
 *
 * <pre>
 * /status                     # The data will be {@link PublishStatus#LOAD_ERROR} or {@link PublishStatus#SUBSCRIBER_ERROR}
 * /cause                      # The data will be one of {@link PublishCause}
 * /time                       # The data will be an ISO-8601 string representing the time of the publish attempt
 * /errors                     # A node containing error_N nodes where N is a number ranging from 0 to one less than the total errors
 * /errors/error_N/message     # A short description of the error
 * /errors/error_N/stacktrace  # The stack trace of the error's exception, if any. If no exception was caught, this node will not exist.
 * </pre>
 *
 * The nodes under {@code /<namespace>/attempts/<serverIpAddress>} will always reflect the latest reload attempt.
 * <p>
 * <strong>NOTE:</strong> It is crucial that separate {@link ZkPojoPublisherImpl} instances on the same server are created with unique namespaces in order to
 * prevent the same {@code /<namespace>/attempts/<serverIpAddress>} node and its children from being modified by multiple publishers.
 */
public class ZkPojoPublisherImpl<T> implements ZkPojoPublisher<T> {
    
    private static final Logger log = LoggerFactory.getLogger(ZkPojoPublisherImpl.class);
    
    public static final String NODE_PATH = "/path";
    public static final String NODE_TRIGGER = "/trigger";
    public static final String NODE_ATTEMPTS = "/attempts";
    public static final String NODE_CAUSE = "/cause";
    public static final String NODE_STATUS = "/status";
    public static final String NODE_ERRORS = "/errors";
    public static final String NODE_ERROR_BASE = "/error_";
    public static final String NODE_MESSAGE = "/message";
    public static final String NODE_STACKTRACE = "/stacktrace";
    public static final String NODE_TIME = "/time";
    
    /**
     * Mapper for JSON files.
     */
    private static final JsonMapper jsonMapper = new JsonMapper();
    
    /**
     * Mapper for XML files.
     */
    // Ensure the mapper is created with factories that support the StAX2 API.
    private static final XmlMapper xmlMapper = new XmlMapper(new WstxInputFactory(), new WstxOutputFactory());
    
    /**
     * Mapper for YAML files.
     */
    private static final YAMLMapper yamlMapper = new YAMLMapper();
    
    /**
     * Map of format names to the mappers.
     */
    // @formatter:off
    private static final Map<String,ObjectMapper> formatToMapper = Map.of(
                    jsonMapper.getFactory().getFormatName(), jsonMapper,
                    xmlMapper.getFactory().getFormatName(), xmlMapper,
                    yamlMapper.getFactory().getFormatName(), yamlMapper
    );
    // @formatter:on
    
    /**
     * Helper class to detect the format of a file.
     */
    private static final DataFormatDetector formatDetector = new DataFormatDetector(jsonMapper.getFactory(), xmlMapper.getFactory(), yamlMapper.getFactory());
    
    /**
     * The finalized path for the node {@code <namespace>/attempts/<serverIpAddress>}.
     */
    private final String baseAttemptNode;
    
    /**
     * The finalized path for the node {@code <namespace>/attempts/<serverIpAddress>/cause}.
     */
    private final String attemptCauseNode;
    
    /**
     * The finalized path for the node {@code <namespace>/attempts/<serverIpAddress>/status}.
     */
    private final String attemptStatusNode;
    
    /**
     * The finalized path for the node {@code <namespace>/attempts/<serverIpAddress>/errors}.
     */
    private final String attemptErrorsNode;
    
    /**
     * The finalized path for the node {@code <namespace>/attempts/<serverIpAddress>/time}.
     */
    private final String attemptTimeNode;
    
    /**
     * The hadoop configuration for reading files from HDFS.
     */
    private final Configuration hadoopConfig;
    
    /**
     * The POJO type.
     */
    private final Class<T> pojoClass;
    
    /**
     * The list of subscribers that should be supplied with new objects after successful reloads.
     */
    private final List<Consumer<Object>> subscribers = new CopyOnWriteArrayList<>();
    
    /**
     * A {@link CuratorCache} that will listen for creates and modifications of the node {@code <namespace>/path}.
     */
    private CuratorCache pathCache;
    
    /**
     * A {@link CuratorCache} that will listen for creates, modifications, and deletions of the node {@code <namespace>/trigger}
     */
    private CuratorCache triggerCache;
    
    /**
     * A boolean that will be set to true when {@link #pathCache} is initialized.
     */
    private final AtomicBoolean pathCacheInitialized = new AtomicBoolean(false);
    
    /**
     * A boolean that will be set to true when {@link #triggerCache} is initialized.
     */
    private final AtomicBoolean triggerCacheInitialized = new AtomicBoolean(false);
    
    /**
     * The lock that must be obtained by any task calling {@link #triggerReload(PublishCause)} in order to perform a reload.
     */
    private final Lock reloadLock = new ReentrantLock();
    
    /**
     * An executor that runs 1 task, and keeps at most 1 in the queue. If a 3rd task arrives, the one in the queue is discarded for the new one. If a bunch of
     * reloads occur, we are only interested in supplying listeners with the latest reload attempt.
     */
    // @formatter:off
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    1, // Use a core pool size of 1.
                    1, // The maximum pool size is 1.
                    0L, TimeUnit.MILLISECONDS, // Keep alive time of 1 ms for idle threads.
                    new ArrayBlockingQueue<>(1), // Only allow 1 task to be queued at a time.
                    new ThreadPoolExecutor.DiscardOldestPolicy()); // If a new task is submitted, discard any task present in the queue.
    // @formatter:on
    
    /**
     * The client.
     */
    private final CuratorFramework client;
    
    public ZkPojoPublisherImpl(ZkClientBuilder zkClientBuilder, String hdfsConfigUrls, Class<T> pojoClass)
                    throws Exception {
        Preconditions.checkNotNull(zkClientBuilder, "zkClientBuilder must not be null");
        Preconditions.checkNotNull(pojoClass, "pojoClass must not be null");
        
        if (log.isDebugEnabled()) {
            log.debug("Initializing with zkClientBuilder={}, hdfsConfigUrls={}, pojoClass={}", zkClientBuilder, hdfsConfigUrls, pojoClass);
        }
        
        this.pojoClass = pojoClass;
        
        try {
            // Load any provided hadoop configurations.
            this.hadoopConfig = new Configuration();
            if (hdfsConfigUrls != null && !hdfsConfigUrls.isBlank()) {
                for (String url : StringUtils.split(hdfsConfigUrls, ",")) {
                    hadoopConfig.addResource(new URL(url));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load hadoop configuration from URLs " + hdfsConfigUrls, e);
        }
        
        // Construct the finalized attempt node paths to be relative to the server IP address.
        String serverIpAddress;
        try {
            serverIpAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get local host address", e);
        }
        
        baseAttemptNode = NODE_ATTEMPTS + "/" + serverIpAddress;
        attemptCauseNode = baseAttemptNode + NODE_CAUSE;
        attemptStatusNode = baseAttemptNode + NODE_STATUS;
        attemptErrorsNode = baseAttemptNode + NODE_ERRORS;
        attemptTimeNode = baseAttemptNode + NODE_TIME;
        
        // Create the Zookeeper client and wait for it to connect to Zookeeper.
        this.client = zkClientBuilder.buildAndStart(3, TimeUnit.MINUTES);
        
        try {
            // Create the caches.
            this.pathCache = createCache(NODE_PATH, client, () -> createPathCacheListener(pathCacheInitialized));
            this.triggerCache = createCache(NODE_TRIGGER, client, () -> createTriggerCacheListener(triggerCacheInitialized));
        } catch (Exception e) {
            log.error("Failed to initialize caches", e);
            // If an error occurs, ensure we close the client and caches to release it.
            close();
            throw e;
        }
    }
    
    /**
     * Create and return a new {@link CuratorCache} that will watch for events concerning the given node, and supply them to the listener returned by the
     * listener supplier.
     *
     * @param node
     *            the node
     * @param listenerSupplier
     *            the listener supplier
     * @return the new cache
     */
    private CuratorCache createCache(String node, CuratorFramework client, Supplier<CuratorCacheListener> listenerSupplier) {
        try {
            CuratorCache cache = CuratorCache.build(client, node, CuratorCache.Options.SINGLE_NODE_CACHE);
            // Add the desired listeners to the cache.
            CuratorCacheListener cacheListener = listenerSupplier.get();
            cache.listenable().addListener(cacheListener);
            // Start the cache.
            cache.start();
            return cache;
        } catch (Exception e) {
            log.error("Failed to create curator cache for path node {}", node, e);
            throw new RuntimeException("Failed to create curator cache for path " + node, e);
        }
    }
    
    /**
     * Create and return a {@link CuratorCacheListener} that will listen for creations and modifications of the node {@code <namespace>/path}, and trigger a
     * configuration reload if the updated {@code <namespace>/path} node has non-empty data. The listener will also set the given boolean to true when its
     * wrapping {@link CuratorCache} is initialized.
     *
     * @param initFlag
     *            a flag to set to true when an initialized event is received by the listener
     * @return the cache listener
     */
    private CuratorCacheListener createPathCacheListener(AtomicBoolean initFlag) {
        // @formatter:off
        return CuratorCacheListener.builder()
                        .afterInitialized() // Ignore any events that occurred before the cache was initialized.
                        .forInitialized(() -> initFlag.set(true)) // Indicate when the cache is initialized.
                        .forCreates((node) -> {
                            byte[] data = node.getData();
                            // Only trigger a reload attempt if the data is not empty.
                            if (data != null && data.length > 0) {
                                executor.submit(()-> triggerReload(PublishCause.PATH_NODE_CREATED));
                            }
                        })
                        .forChanges((oldNode, newNode) -> {
                            byte[] newData = newNode.getData();
                            // Only trigger a reload attempt if the data is not empty.
                            if(newData != null && newData.length > 0) {
                                executor.submit(()-> triggerReload(PublishCause.PATH_NODE_MODIFIED));
                            }
                            
                        }).build();
        // @formatter:on
    }
    
    /**
     * Create and return a {@link CuratorCacheListener} that will listen for creations, modifications, and deletions of the node {@code /trigger}, and trigger a
     * configuration reload. The listener will also set the given boolean to true when its wrapping {@link CuratorCache} is initialized.
     *
     * @param initFlag
     *            a flag to set to true when an initialized event is received by the listener
     */
    private CuratorCacheListener createTriggerCacheListener(AtomicBoolean initFlag) {
        // @formatter:off
        return CuratorCacheListener.builder()
                        .afterInitialized() // Ignore any events that occurred before the cache was initialized.
                        .forInitialized(() -> initFlag.set(true)) // Indicate when the cache is initialized.
                        .forCreates((node) -> executor.submit(()-> triggerReload(PublishCause.TRIGGER_NODE_CREATED)))
                        .forChanges((oldNode, newNode) -> executor.submit(() -> triggerReload(PublishCause.TRIGGER_NODE_MODIFIED)))
                        .forDeletes((node) -> executor.submit(() -> triggerReload(PublishCause.TRIGGER_NODE_DELETED)))
                        .build();
        // @formatter:on
    }
    
    /**
     * Return whether the underlying node caches are initialized and ready to listen for events.
     *
     * @return true if all underlying caches are initialized, or false otherwise
     */
    public boolean areCachesInitialized() {
        return pathCacheInitialized.get() && triggerCacheInitialized.get();
    }
    
    /**
     * Trigger a POJO reload. If a POJO is reloaded, it will be provided to any listeners configured for this {@link ZkPojoPublisherImpl}.
     *
     * @param cause
     *            the triggering cause
     */
    private void triggerReload(PublishCause cause) {
        if (log.isDebugEnabled()) {
            log.debug("Reload triggered by {}", cause);
        }
        
        // Obtain the reload lock.
        reloadLock.lock();
        try {
            Instant attemptTime = Instant.now();
            // Attempt to load a new POJO instance.
            PublishResult result = getPojoFromZk();
            
            // If we successfully loaded a valid subscriber, pass it to any subscribers registered with this updater.
            if (result.getStatus() == PublishStatus.SUCCESS) {
                if (!subscribers.isEmpty()) {
                    List<Exception> subscriberExceptions = new ArrayList<>();
                    for (Consumer<Object> subscriber : subscribers) {
                        try {
                            subscriber.accept(result.getUpdatedObject());
                        } catch (Exception e) {
                            // If an exception is thrown by a subscriber, log it and record it in the status.
                            log.warn("Exception thrown by subscriber {}", subscriber, e);
                            subscriberExceptions.add(e);
                        }
                    }
                    log.debug("Supplied object update to all subscribers");
                    if (!subscriberExceptions.isEmpty()) {
                        result = PublishResult.subscriberErrors(result.getTime(), result.getUpdatedObject(), subscriberExceptions);
                    }
                } else {
                    log.debug("No subscribers registered to be supplied updates");
                }
            }
            
            if (log.isDebugEnabled()) {
                log.debug("Update of {} completed with attemptTime={}, trigger={}, status={}, errors={}", pojoClass.getName(), attemptTime, cause,
                                result.getStatus(), result.getErrors());
            }
            
            // Update the attempt nodes for the latest attempt.
            updateAttemptNodes(cause, result.getStatus(), result.getErrors(), attemptTime);
        } catch (Exception e) {
            log.error("Failed to load instance of {}", pojoClass.getName(), e);
            throw new RuntimeException("Failed to load instance of " + pojoClass.getName(), e);
        } finally {
            reloadLock.unlock();
        }
    }
    
    /**
     * Make the following changes underneath the namespace configured for this {@link ZkPojoPublisherImpl}. All nodes listed here will be created if they do not
     * exist:
     * <ul>
     * <li>Set the data for the node {@code /attempts/<serverIpAddress>/status} to the bytes of the string form of the given {@link PublishStatus}.</li>
     * <li>Set the data for the node {@code /attempts/<serverIpAddress>/cause} to the bytes of the string form of the given {@link PublishCause}.</li>
     * <li>Set the data for the node {@code /attempts/<serverIpAddress>/time} to the bytes of the string form of the given {@link Instant}.</li>
     * <li>Depending on the list of error messages provided, make the following changes to {@code /attempts/<serverIpAddress>/errors}:
     * <ul>
     * <li>If the error messages list is empty, delete the node {@code /attempts/<serverIdAddress>/errors} and its children.</li>
     * <li>If the error messages list is not empty, set the children of the node {@code /attempts/<serverIpAddress>/errors} such that there is one child for
     * each error message, with the path {@code error_X} where X equals the index of the error message in the list, and data is set to the bytes of the error
     * message.</li>
     * </ul>
     * </li>
     * </ul>
     *
     * @param cause
     *            the triggering event for the reload
     * @param status
     *            the status
     * @param errors
     *            the errors
     * @param time
     *            the time of the attempt
     * @throws Exception
     *             if an error occurs on Zookeeper
     */
    private void updateAttemptNodes(PublishCause cause, PublishStatus status, List<PublishError> errors, Instant time)
                    throws Exception {
        // Ensure the base /reload node is created.
        client.createContainers(baseAttemptNode);
        
        setData(client, attemptCauseNode, cause.toString().getBytes(StandardCharsets.UTF_8));
        setData(client, attemptStatusNode, status.toString().getBytes(StandardCharsets.UTF_8));
        updateErrorsNode(client, errors);
        setData(client, attemptTimeNode, time.toString().getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * Update the node {@code /attempts/<serverIpAddress>/errors} to reflect the contents of the given error list.
     *
     * @param client
     *            the client
     * @param errors
     *            the errors
     * @throws Exception
     *             if an error occurs in Zookeeper
     */
    private void updateErrorsNode(CuratorFramework client, List<PublishError> errors) throws Exception {
        Stat stat = client.checkExists().forPath(attemptErrorsNode);
        if (stat != null) {
            client.delete().deletingChildrenIfNeeded().forPath(attemptErrorsNode);
        }
        if (!errors.isEmpty()) {
            client.create().forPath(attemptErrorsNode);
            for (int i = 0; i < errors.size(); i++) {
                PublishError error = errors.get(i);
                
                String errorNode = attemptErrorsNode + NODE_ERROR_BASE + i;
                client.create().forPath(errorNode);
                
                String messageNode = errorNode + NODE_MESSAGE;
                setData(client, messageNode, error.getMessage().getBytes(StandardCharsets.UTF_8));
                
                if (error.hasException()) {
                    String stacktraceNode = errorNode + NODE_STACKTRACE;
                    setData(client, stacktraceNode, ExceptionUtils.getStackTrace(error.getException()).getBytes(StandardCharsets.UTF_8));
                }
            }
        }
    }
    
    /**
     * Set the data for the given node.
     *
     * @param node
     *            the path to the node.
     * @param data
     *            the data to set
     * @throws Exception
     *             if an error occurs on Zookeeper
     */
    private void setData(CuratorFramework client, String node, byte[] data) throws Exception {
        Stat stat = client.checkExists().forPath(node);
        if (stat == null) {
            client.create().forPath(node, data);
        } else {
            client.setData().forPath(node, data);
        }
    }
    
    /**
     * Attempt to load a new POJO from the path specified in the data of the node {@value NODE_PATH} under the zookeeper namespace configured for this
     * {@link ZkPojoPublisherImpl}. The path may point to an http, hdfs, or local file. Note that an invocation of this method will not result in the object being
     * supplied to any subscribers, nor will the attempt result be recorded to Zookeeper.
     *
     * @return the result
     */
    public PublishResult getPojoFromZk() {
        if (log.isDebugEnabled()) {
            log.debug("Attempting to load new instance of {} from filepath in {}", pojoClass.getName(), NODE_PATH);
        }
        
        Instant attemptTime = Instant.now();
        try {
            // Verify that the path node exists.
            Stat stat = client.checkExists().forPath(NODE_PATH);
            if (stat == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Node {} does not exist, skipping reload", NODE_PATH);
                }
                return PublishResult.error(attemptTime, "Node does not exist: " + NODE_PATH);
            }
            
            // Fetch the path from the path node.
            byte[] pathBytes = client.getData().forPath(NODE_PATH);
            
            // Verify we have a non-blank path.
            if (pathBytes == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Node {} does not have any data, skipping reload", NODE_PATH);
                }
                return PublishResult.error(attemptTime, "File path not set in data for node " + NODE_PATH);
            }
            
            String path = new String(pathBytes, StandardCharsets.UTF_8);
            if (path.isBlank()) {
                if (log.isDebugEnabled()) {
                    log.debug("Node {}} does not have a non-blank filepath, skipping reload", NODE_PATH);
                }
                return PublishResult.error(attemptTime, "Blank filepath set in data for node " + NODE_PATH);
            }
            
            // Trim the path of any leading/trailing whitespace.
            path = path.trim();
            
            // Read the contents of the file.
            byte[] contents;
            try {
                contents = getFileContents(path);
            } catch (NoSuchFileException e) {
                log.error("Failed to read contents from file {}", path, e);
                return PublishResult.error(attemptTime, "File not found: " + path, e);
            } catch (Exception e) {
                log.error("Failed to read contents from file {}", path, e);
                return PublishResult.error(attemptTime, "Failed to read contents from file " + path + ": " + e.getMessage(), e);
            }
            
            // Determine the format (XML, JSON, YAML) and use the corresponding mapper to deserialize the contents.
            Object pojo;
            DataFormatMatcher format = formatDetector.findFormat(contents);
            if (format.hasMatch()) {
                JsonFactory factory = format.getMatch();
                try {
                    // Deserialize the POJO using the associated mapper for the format.
                    pojo = formatToMapper.get(factory.getFormatName()).readValue(contents, pojoClass);
                } catch (Exception e) {
                    log.error("Failed to deserialize file {} to a {}", path, pojoClass.getName(), e);
                    return PublishResult.error(attemptTime, "Failed to deserialize file to a " + pojoClass.getName(), e);
                }
            } else {
                // If we do not have a match for a supported mapper, return an error.
                if (log.isDebugEnabled()) {
                    log.debug("File {} could not be detected as XML, JSON, or YAML, skipping reload", path);
                }
                return PublishResult.error(attemptTime, "File " + path + " must be XML, JSON, or YAML");
            }
            
            return PublishResult.success(attemptTime, pojo);
        } catch (Exception e) {
            log.error("Failed to reload new instance of {} from Zookeeper", pojoClass.getName(), e);
            throw new RuntimeException("Failed to load new instance of " + pojoClass.getName() + " from Zookeeper", e);
        }
    }
    
    /**
     * Return an {@link InputStream} for the file on the given path. The path will be examined and handled based on the following schemes:
     * <ul>
     * <li>{@code http://} or {@code https://}: The path will be treated as a URL.</li>
     * <li>{@code hdfs://}: The path will be treated as an HDFS filepath.</li>
     * <li>{@code file://} or none: The path will be treated as a local filepath.</li>
     * </ul>
     *
     * @param path
     *            the path
     * @return the new {@link InputStream}
     * @throws IOException
     *             if an {@link InputStream} cannot be created
     */
    private byte[] getFileContents(String path) throws IOException {
        URI uri = URI.create(path);
        String scheme = uri.getScheme();
        if (scheme != null) {
            if (scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https")) {
                return getContentFromURL(path);
            } else if (scheme.equalsIgnoreCase("hdfs")) {
                return getContentFromHdfs(uri.getPath());
            } else if (scheme.equalsIgnoreCase("file")) {
                return getContentFromLocalFs(uri.getPath());
            } else {
                throw new IOException("Unsupported URI scheme: " + scheme);
            }
        } else {
            return getContentFromLocalFs(uri.getPath());
        }
    }
    
    /**
     * Return the contents of the file on the given URL.
     *
     * @param path
     *            the URL
     * @return the {@link InputStream}
     * @throws IOException
     *             if an {@link InputStream} cannot be created
     */
    private byte[] getContentFromURL(String path) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Attempting to read file from URL: {}", path);
        }
        URL url = new URL(path);
        try (InputStream is = url.openStream()) {
            return IOUtils.toByteArray(is);
        }
    }
    
    /**
     * Return the contents of the file on HDFS.
     *
     * @param path
     *            the filepath
     * @return the {@link InputStream}
     * @throws IOException
     *             if an {@link InputStream} cannot be created
     */
    private byte[] getContentFromHdfs(String path) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Attempting to read file from HDFS: {}", path);
        }
        FileSystem fileSystem = FileSystem.get(hadoopConfig);
        try (InputStream is = fileSystem.open(new org.apache.hadoop.fs.Path(path))) {
            return IOUtils.toByteArray(is);
        }
    }
    
    /**
     * Return the contents of the file on the local filesystem.
     *
     * @param path
     *            the filepath
     * @return the {@link InputStream}
     * @throws IOException
     *             if an {@link InputStream} cannot be created
     */
    private byte[] getContentFromLocalFs(String path) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Attempting to read file from local filesystem: {}", path);
        }
        try (InputStream is = Files.newInputStream(Path.of(path), StandardOpenOption.READ)) {
            return IOUtils.toByteArray(is);
        }
    }
    
    /**
     * Add a {@link Consumer} that, when a new POJO is loaded a path specified in Zookeeper, will be provided that configuration.
     *
     * @param subscriber
     *            the subscriber
     */
    public void subscribeToUpdates(Consumer<Object> subscriber) {
        this.subscribers.add(subscriber);
    }
    
    /**
     * Clean up resources used by this {@link ZkPojoPublisherImpl}. Performs the following tasks:
     * <ul>
     * <li>Close the curator caches for the nodes {@value #NODE_PATH} and @value #NODE_TRIGGER}.</li>
     * <li>Shut down the executor service that executes reload tasks.</li>
     * <li>Clear the subscriber list.</li>
     * </ul>
     */
    public void close() {
        if (pathCache != null) {
            try {
                pathCache.close();
            } catch (Exception e) {
                log.warn("Failed to close path cache", e);
            } finally {
                pathCache = null;
            }
        }
        if (triggerCache != null) {
            try {
                triggerCache.close();
            } catch (Exception e) {
                log.warn("Failed to close trigger cache", e);
            } finally {
                triggerCache = null;
            }
        }
        try {
            executor.shutdown();
            boolean terminated = executor.awaitTermination(1, TimeUnit.MINUTES);
            if (!terminated) {
                log.warn("Closed executor, but not all threads completed within 1 minute");
            }
        } catch (Exception e) {
            log.warn("Failed to close executor", e);
        }
        
        try {
            subscribers.clear();
        } catch (Exception e) {
            log.warn("Failed to clear subscribers", e);
        }
        
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Failed to close Zookeeper client", e);
            }
        }
    }
    
    public enum PublishCause {
        /**
         * Indicates the triggering event was the creation of the node {@value ZkPojoPublisherImpl#NODE_PATH} with non-empty data.
         */
        PATH_NODE_CREATED,
        /**
         * Indicates the triggering event was the modification of the node {@value ZkPojoPublisherImpl#NODE_PATH} with non-empty data.
         */
        PATH_NODE_MODIFIED,
        /**
         * Indicates the triggering event was the creation of the node {@value ZkPojoPublisherImpl#NODE_TRIGGER}.
         */
        TRIGGER_NODE_CREATED,
        /**
         * Indicates the triggering event was the modification of the node {@value ZkPojoPublisherImpl#NODE_TRIGGER}.
         */
        TRIGGER_NODE_MODIFIED,
        /**
         * Indicates the triggering event was the deletion of the node {@value ZkPojoPublisherImpl#NODE_TRIGGER}.
         */
        TRIGGER_NODE_DELETED
    }
    
    
    public enum PublishStatus {
        /**
         * Indicates an object update was successfully loaded from Zookeeper and, if triggered by a trigger event, successfully published to all subscribers.
         */
        SUCCESS,
        
        /**
         * Indicates an error occurred when trying to load an object update from Zookeeper.
         */
        LOAD_ERROR,
        
        /**
         * Indicates an object update was successfully loaded from Zookeeper, but one or more subscribers threw an error when provided the updated object.
         */
        SUBSCRIBER_ERROR
    }
    
    /**
     * Represents an error that occurred when attempting to load a new updated object via a {@link ZkPojoPublisherImpl}.
     */
    public static class PublishError {
        
        /**
         * A short description of the error.
         */
        private final String message;
        
        /**
         * The associated exception for the error, if any.
         */
        private final Exception exception;
        
        public PublishError(String message, Exception exception) {
            this.message = message;
            this.exception = exception;
        }
        
        public String getMessage() {
            return message;
        }
        
        public Exception getException() {
            return exception;
        }
        
        public boolean hasException() {
            return exception != null;
        }
    }
    
    /**
     * Represents a result from {@link ZkPojoPublisherImpl#getPojoFromZk()}.
     */
    public static class PublishResult {
        
        /**
         * The updated object. This will be null if no object update could be successfully loaded.
         */
        private final Object updatedObject;
        
        /**
         * The status of loading the object.
         */
        private final PublishStatus status;
        
        /**
         * A list of any errors that occurred while trying to load the results.
         */
        private final List<PublishError> errors;
        
        /**
         * The time that loading the object was attempted.
         */
        private final Instant time;
        
        public static PublishResult success(Instant time, Object pojo) {
            return new PublishResult(pojo, PublishStatus.SUCCESS, null, time);
        }
        
        public static PublishResult error(Instant time, String message) {
            return new PublishResult(null, PublishStatus.LOAD_ERROR, List.of(new PublishError(message, null)), time);
        }
        
        public static PublishResult error(Instant time, String message, Exception exception) {
            return new PublishResult(null, PublishStatus.LOAD_ERROR, List.of(new PublishError(message, exception)), time);
        }
        
        public static PublishResult subscriberErrors(Instant time, Object pojo, List<Exception> exceptions) {
            List<PublishError> errors = exceptions.stream().map((e) -> new PublishError("Exception thrown by listener: " + e.getMessage(), e))
                            .collect(Collectors.toList());
            return new PublishResult(pojo, PublishStatus.SUBSCRIBER_ERROR, errors, time);
        }
        
        public PublishResult(Object updatedObject, PublishStatus status, List<PublishError> errors, Instant time) {
            this.updatedObject = updatedObject;
            this.status = status;
            this.errors = errors != null ? List.copyOf(errors) : List.of();
            this.time = time;
        }
        
        public Object getUpdatedObject() {
            return updatedObject;
        }
        
        public PublishStatus getStatus() {
            return status;
        }
        
        public List<PublishError> getErrors() {
            return errors;
        }
        
        public Instant getTime() {
            return time;
        }
        
        @Override
        public String toString() {
            return new StringJoiner(", ", PublishResult.class.getSimpleName() + "[", "]").add("updatedObject=" + updatedObject).add("status=" + status)
                            .add("errors=" + errors).add("time=" + time).toString();
        }
    }
    
}
