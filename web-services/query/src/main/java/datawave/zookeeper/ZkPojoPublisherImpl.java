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
import java.util.Objects;
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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctc.wstx.stax.WstxInputFactory;
import com.ctc.wstx.stax.WstxOutputFactory;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.format.DataFormatDetector;
import com.fasterxml.jackson.core.format.DataFormatMatcher;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import datawave.ingest.util.ThreadUtil;
import datawave.util.StringUtils;

/**
 * A publisher that can be triggered to deserialize and publish updates of a configured class to listeners. The publisher leverages Zookeeper and is triggered
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
 * provided to all listeners that have subscribed to the publisher via {@link ZkPojoPublisherImpl#addListener(Consumer)}. The status of any triggered attempt
 * will be recorded under the node {@code /<namespace>/attempts/<serverIpAddress>/latest}. The data of the node will contain a {@link PublishAttempt} as JSON.
 * The data will always reflect the latest reload attempt.
 *
 * <p>
 * <strong>NOTE:</strong> It is crucial that separate {@link ZkPojoPublisherImpl} instances on the same server are created with unique namespaces in order to
 * prevent the same {@code /<namespace>/attempts/<serverIpAddress>} node and its children from being modified by multiple publishers.
 */
public class ZkPojoPublisherImpl<T> implements ZkPojoPublisher<T> {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    public static final String NODE_PATH = "/path";
    public static final String NODE_TRIGGER = "/trigger";
    public static final String NODE_ATTEMPTS = "/attempts";
    public static final String NODE_LATEST = "/latest";

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

    private ZkClientBuilder zkClientBuilder;

    private String hdfsConfigUrls;

    /**
     * The POJO type.
     */
    private final Class<T> pojoClass;

    /**
     * The finalized path for the node {@code <namespace>/attempts/<serverIpAddress>/latest}
     */
    private String latestAttemptNode;

    /**
     * The hadoop configuration for reading files from HDFS.
     */
    private Configuration hadoopConfig;

    /**
     * The listeners that should be supplied with new objects after successful reloads.
     */
    private final List<Consumer<T>> listeners = new CopyOnWriteArrayList<>();

    /**
     * The client.
     */
    private CuratorFramework zkClient;

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
     * The lock that must be obtained by {@link #setup()} or {@link #shutdown()}, or by any task calling {@link #reloadAndPublish(Trigger)} in order to perform
     * a reload.
     */
    private final Lock publisherLock = new ReentrantLock();

    /**
     * Whether this {@link ZkPojoPublisherImpl} has been initialized via {@link #setup()}. This will be set to false by {@link #shutdown()}.
     */
    private final AtomicBoolean ready = new AtomicBoolean(false);

    /**
     * An executor that runs 1 task, and keeps at most 1 in the queue. If a 3rd task arrives, the one in the queue is discarded for the new one. If a bunch of
     * reloads occur, we are only interested in supplying listeners with the latest reload attempt.
     */
    private ThreadPoolExecutor executor;

    public ZkPojoPublisherImpl(Class<T> pojoClass) {
        this.pojoClass = pojoClass;
    }

    /**
     * Set the Zookeeper client builder
     *
     * @param zkClientBuilder
     *            the builder
     */
    public void setZkClientBuilder(ZkClientBuilder zkClientBuilder) {
        this.zkClientBuilder = zkClientBuilder;
    }

    /**
     * Set the hadoop configuration file paths
     *
     * @param hdfsConfigUrls
     *            the paths
     */
    public void setHdfsConfigUrls(String hdfsConfigUrls) {
        this.hdfsConfigUrls = hdfsConfigUrls;
    }

    /**
     * Initialize this {@link ZkPojoPublisherImpl}, starting up the Zookeeper client and caches as needed.
     */
    public void setup() {
        if (log.isDebugEnabled()) {
            log.debug("Setting up with zkClientBuilder={}, hdfsConfigUrls={}, pojoClass={}", zkClientBuilder, hdfsConfigUrls, pojoClass);
        }
        publisherLock.lock();
        try {
            // Avoid potentially starting the client and caches multiple times if they are already started.
            if (!ready.get()) {
                if (this.executor == null) {
                    // Create an executor that runs 1 task, and keeps at most 1 in the queue. If a 3rd task arrives, the one in the queue is discarded for the
                    // new
                    // one.
                    // If a bunch of reloads occur, we are only interested in supplying listeners with the latest reload attempt.
                    // @formatter:off
                    this.executor = new ThreadPoolExecutor(
                            1, // Use a core pool size of 1.
                            1, // The maximum pool size is 1.
                            0L, TimeUnit.MILLISECONDS, // Keep alive time of 1 ms for idle threads.
                            new ArrayBlockingQueue<>(1), // Only allow 1 task to be queued at a time.
                            new ThreadPoolExecutor.DiscardOldestPolicy()); // If a new task is submitted, discard any task present in the queue.
                    // @formatter:on
                }

                // Load the hadoop configurations.
                if (hadoopConfig == null) {
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
                }

                // Create the Zookeeper client.
                if (this.zkClient == null) {
                    try {
                        this.zkClient = zkClientBuilder.buildAndStart(3, TimeUnit.MINUTES);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to create Zookeeper client", e);
                    }
                }

                // Create the path cache.
                if (this.pathCache == null) {
                    try {
                        this.pathCache = createCache(NODE_PATH, zkClient, () -> createPathCacheListener(pathCacheInitialized));
                        this.pathCache.start();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to create path cache", e);
                    }
                }

                // Create the trigger cache.
                try {
                    this.triggerCache = createCache(NODE_TRIGGER, zkClient, () -> createTriggerCacheListener(triggerCacheInitialized));
                    this.triggerCache.start();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create trigger cache", e);
                }

                // Ensure the caches are initialized before marking the publisher as ready.
                boolean cachesInitialized = ThreadUtil.blockUntil(TimeUnit.SECONDS.toMillis(5), 500,
                                () -> pathCacheInitialized.get() && triggerCacheInitialized.get());
                if (!cachesInitialized) {
                    throw new IllegalStateException("Failed to initialize path and trigger caches within 5 seconds");
                }

                // Configure the base path for the latest attempt node.
                if (latestAttemptNode == null) {
                    try {
                        String serverIpAddress = InetAddress.getLocalHost().getHostAddress();
                        latestAttemptNode = NODE_ATTEMPTS + "/" + serverIpAddress + NODE_LATEST;
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to determine path for latest node", e);
                    }
                }

                this.ready.set(true);
            }
        } catch (Exception e) {
            log.error("Failed to set up Zookeeper publisher", e);
            shutdown();
        } finally {
            publisherLock.unlock();
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
                                executor.execute(()-> reloadAndPublish(Trigger.PATH_NODE_CREATED));
                            }
                        })
                        .forChanges((oldNode, newNode) -> {
                            byte[] newData = newNode.getData();
                            // Only trigger a reload attempt if the data is not empty.
                            if(newData != null && newData.length > 0) {
                                executor.execute(()-> reloadAndPublish(Trigger.PATH_NODE_MODIFIED));
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
                        .forCreates((node) -> executor.execute(()-> reloadAndPublish(Trigger.TRIGGER_NODE_CREATED)))
                        .forChanges((oldNode, newNode) -> executor.execute(() -> reloadAndPublish(Trigger.TRIGGER_NODE_MODIFIED)))
                        .forDeletes((node) -> executor.execute(() -> reloadAndPublish(Trigger.TRIGGER_NODE_DELETED)))
                        .build();
        // @formatter:on
    }

    /**
     * Trigger a POJO reload. If a POJO is reloaded, it will be provided to any listeners configured for this {@link ZkPojoPublisherImpl}.
     *
     * @param trigger
     *            the triggering cause
     */
    private void reloadAndPublish(Trigger trigger) {
        if (log.isDebugEnabled()) {
            log.debug("Reload triggered by {}", trigger);
        }

        // Obtain the reload lock.
        publisherLock.lock();
        try {
            Instant attemptTime = Instant.now();
            // Attempt to load a new POJO instance.
            PojoResult<T> result = getPojoFromZk();

            // If we successfully loaded a valid listener, pass it to any listeners registered with this updater.
            if (result.getStatus() == Status.SUCCESS) {
                if (!listeners.isEmpty()) {
                    List<Error> listenerErrors = new ArrayList<>();
                    log.debug("Supplying pojo update to listeners");
                    for (Consumer<T> listener : listeners) {
                        try {
                            listener.accept(result.getPojo());
                        } catch (Throwable t) {
                            // If an exception is thrown by a listener, log it and record it in the status.
                            log.error("Exception thrown by listener {} when provided updated instance of {}", listener.getClass().getName(),
                                            pojoClass.getName(), t);
                            listenerErrors.add(Error.of("Exception thrown by listener " + listener.getClass().getName(), t));
                        }
                    }
                    log.debug("Supplied pojo update to all listeners");
                    if (!listenerErrors.isEmpty()) {
                        result = PojoResult.listenerErrors(result.getTime(), result.getPojo(), listenerErrors);
                    }
                } else {
                    log.debug("No listeners registered to be supplied updates");
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("Update of {} completed with attemptTime={}, trigger={}, status={}, errors={}", pojoClass.getName(), attemptTime, trigger,
                                result.getStatus(), result.getErrors());
            }

            // Update the attempt nodes for the latest attempt.
            updateAttemptNode(attemptTime, trigger, result.getStatus(), result.getErrors());
        } catch (Exception e) {
            log.error("Failed to load instance of {} after reload triggered by {}", pojoClass.getName(), trigger, e);
            throw new RuntimeException("Failed to load instance of " + pojoClass.getName() + " after reload triggered by " + trigger, e);
        } finally {
            publisherLock.unlock();
        }
    }

    /**
     * Update the data of the latest attempt node, creating it if needed.
     *
     * @param time
     *            the time of the attempt
     * @param trigger
     *            the cause of the attempt
     * @param status
     *            the status of the attempt
     * @param errors
     *            the errors of the attempt
     */
    private void updateAttemptNode(Instant time, Trigger trigger, Status status, List<Error> errors) throws Exception {
        PublishAttempt attempt = new PublishAttempt(time.toEpochMilli(), trigger, status, errors);
        try {
            byte[] data = jsonMapper.writeValueAsBytes(attempt);
            Stat stat = zkClient.checkExists().forPath(latestAttemptNode);
            if (stat != null) {
                zkClient.setData().forPath(latestAttemptNode, data);
            } else {
                zkClient.create().creatingParentsIfNeeded().forPath(latestAttemptNode, data);
            }
        } catch (Exception e) {
            log.error("Failed to record attempt {} to Zookeeper", attempt, e);
            throw e;
        }
    }

    /**
     * Attempt to load a new POJO from the path specified in the data of the node {@value NODE_PATH} under the zookeeper namespace configured for this
     * {@link ZkPojoPublisherImpl}. The path may point to an http, hdfs, or local file. Note that an invocation of this method will not result in the pojo being
     * supplied to any listeners, nor will the attempt result be recorded to Zookeeper.
     *
     * @return the result
     */
    private PojoResult<T> getPojoFromZk() {
        if (log.isDebugEnabled()) {
            log.debug("Attempting to load new instance of {} from filepath in {}", pojoClass.getName(), NODE_PATH);
        }

        Instant attemptTime = Instant.now();
        try {
            // Verify that the path node exists.
            Stat stat = zkClient.checkExists().forPath(NODE_PATH);
            if (stat == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Node {} does not exist, skipping reload", NODE_PATH);
                }
                return PojoResult.error(attemptTime, "Node does not exist: " + NODE_PATH);
            }

            // Fetch the path from the path node.
            byte[] pathBytes = zkClient.getData().forPath(NODE_PATH);

            // Verify we have a non-blank path.
            if (pathBytes == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Node {} does not have any data, skipping reload", NODE_PATH);
                }
                return PojoResult.error(attemptTime, "File path not set in data for node " + NODE_PATH);
            }

            String path = new String(pathBytes, StandardCharsets.UTF_8);
            if (path.isBlank()) {
                if (log.isDebugEnabled()) {
                    log.debug("Node {}} does not have a non-blank filepath, skipping reload", NODE_PATH);
                }
                return PojoResult.error(attemptTime, "Blank filepath set in data for node " + NODE_PATH);
            }

            // Trim the path of any leading/trailing whitespace.
            path = path.trim();

            // Read the contents of the file.
            byte[] contents;
            try {
                contents = getFileContents(path);
            } catch (NoSuchFileException e) {
                log.error("Failed to read contents from file {}", path, e);
                return PojoResult.error(attemptTime, "File not found: " + path, e);
            } catch (Exception e) {
                log.error("Failed to read contents from file {}", path, e);
                return PojoResult.error(attemptTime, "Failed to read contents from file " + path + ": " + e.getMessage(), e);
            }

            // Determine the format (XML, JSON, YAML) and use the corresponding mapper to deserialize the contents.
            T pojo;
            DataFormatMatcher format = formatDetector.findFormat(contents);
            if (format.hasMatch()) {
                JsonFactory factory = format.getMatch();
                try {
                    // Deserialize the POJO using the associated mapper for the format.
                    pojo = formatToMapper.get(factory.getFormatName()).readValue(contents, pojoClass);
                } catch (Exception e) {
                    log.error("Failed to deserialize file {} to a {}", path, pojoClass.getName(), e);
                    return PojoResult.error(attemptTime, "Failed to deserialize file to a " + pojoClass.getName(), e);
                }
            } else {
                // If we do not have a match for a supported mapper, return an error.
                if (log.isDebugEnabled()) {
                    log.debug("File {} could not be detected as XML, JSON, or YAML, skipping reload", path);
                }
                return PojoResult.error(attemptTime, "File " + path + " must be XML, JSON, or YAML");
            }

            return PojoResult.success(attemptTime, pojo);
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
     * @param listener
     *            the listener to add
     */
    @Override
    public void addListener(Consumer<T> listener) {
        this.listeners.add(listener);
    }

    /**
     * Remove the given listener by identity.
     *
     * @param listener
     *            the listener to remove
     */
    @Override
    public void removeListener(Consumer<T> listener) {
        this.listeners.removeIf((element) -> element == listener);
    }

    /**
     * Clean up resources used by this {@link ZkPojoPublisherImpl}. Performs the following tasks:
     * <ul>
     * <li>Close the curator caches for the nodes {@value #NODE_PATH} and @value #NODE_TRIGGER}.</li>
     * <li>Closes the Zookeeper client.</li>
     * <li>Shut down the executor service that executes reload tasks.</li>
     * <li>Clear the listener list.</li>
     * </ul>
     */
    public void shutdown() {
        log.debug("Shutting down");
        publisherLock.lock();
        try {
            this.ready.set(false);
            try {
                pathCacheInitialized.set(false);
                pathCache.close();
            } catch (Exception e) {
                log.warn("Failed to close path cache", e);
            } finally {
                pathCache = null;
            }

            try {
                triggerCacheInitialized.set(false);
                triggerCache.close();
            } catch (Exception e) {
                log.warn("Failed to close trigger cache", e);
            } finally {
                triggerCache = null;
            }

            try {
                executor.shutdown();
                boolean terminated = executor.awaitTermination(1, TimeUnit.MINUTES);
                if (!terminated) {
                    log.warn("Closed executor, but not all threads completed within 1 minute");
                }
            } catch (Exception e) {
                log.warn("Failed to close executor", e);
            } finally {
                executor = null;
            }

            try {
                zkClient.close();
            } catch (Exception e) {
                log.warn("Failed to close Zookeeper client", e);
            } finally {
                zkClient = null;
            }

            try {
                listeners.clear();
            } catch (Exception e) {
                log.warn("Failed to clear listeners", e);
            }

            hadoopConfig = null;
            latestAttemptNode = null;
        } finally {
            publisherLock.unlock();
        }
    }

    public enum Trigger {
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

    public enum Status {
        /**
         * Indicates a pojo update was successfully loaded from Zookeeper and, if triggered by a trigger event, successfully published to all listeners.
         */
        SUCCESS,

        /**
         * Indicates an error occurred when trying to load a pojo update from Zookeeper.
         */
        LOAD_ERROR,

        /**
         * Indicates an pojo update was successfully loaded from Zookeeper, but one or more listeners threw an error when provided the updated pojo.
         */
        LISTENER_ERROR
    }

    /**
     * Represents an error, possible originating from an exception.
     */
    public static class Error {

        private final String message;
        private final String stacktrace;

        public static Error of(String message) {
            return new Error(message, null);
        }

        public static Error of(String message, Throwable throwable) {
            return new Error(message, ExceptionUtils.getStackTrace(throwable));
        }

        @JsonCreator
        public Error(@JsonProperty("message") String message, @JsonProperty("stacktrace") String stacktrace) {
            this.message = message;
            this.stacktrace = stacktrace;
        }

        public String getMessage() {
            return message;
        }

        public String getStacktrace() {
            return stacktrace;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Error error = (Error) o;
            return Objects.equals(message, error.message) && Objects.equals(stacktrace, error.stacktrace);
        }

        @Override
        public int hashCode() {
            return Objects.hash(message, stacktrace);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Error.class.getSimpleName() + "[", "]").add("message='" + message + "'").add("stacktrace='" + stacktrace + "'")
                            .toString();
        }
    }

    /**
     * Represents a result from {@link ZkPojoPublisherImpl#getPojoFromZk()}.
     */
    public static class PojoResult<T> {

        /**
         * The updated POJO. This will be null if no pojo update could be successfully loaded.
         */
        private final T pojo;

        /**
         * The status of loading the pojo.
         */
        private final Status status;

        /**
         * A list of any errors that occurred while trying to load the POJO.
         */
        private final List<Error> errors;

        /**
         * The time that loading the pojo was attempted.
         */
        private final Instant time;

        public static <T> PojoResult<T> success(Instant time, T pojo) {
            return new PojoResult<>(pojo, Status.SUCCESS, null, time);
        }

        public static <T> PojoResult<T> error(Instant time, String message) {
            return new PojoResult<>(null, Status.LOAD_ERROR, List.of(Error.of(message)), time);
        }

        public static <T> PojoResult<T> error(Instant time, String message, Exception exception) {
            return new PojoResult<>(null, Status.LOAD_ERROR, List.of(Error.of(message, exception)), time);
        }

        public static <T> PojoResult<T> listenerErrors(Instant time, T pojo, List<Error> errors) {
            return new PojoResult<>(pojo, Status.LISTENER_ERROR, errors, time);
        }

        public PojoResult(T pojo, Status status, List<Error> errors, Instant time) {
            this.pojo = pojo;
            this.status = status;
            this.errors = errors != null ? List.copyOf(errors) : List.of();
            this.time = time;
        }

        public T getPojo() {
            return pojo;
        }

        public Status getStatus() {
            return status;
        }

        public List<Error> getErrors() {
            return errors;
        }

        public Instant getTime() {
            return time;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", PojoResult.class.getSimpleName() + "[", "]").add("pojo=" + pojo).add("status=" + status).add("errors=" + errors)
                            .add("time=" + time).toString();
        }
    }

    /**
     * Represents a triggered publishing attempt.
     */
    public static class PublishAttempt {

        /**
         * The epoch time in milliseconds that a POJO publish was attempted.
         */
        private final long timestamp;

        /**
         * The action that triggered the POJO publish attempt.
         */
        private final Trigger trigger;

        /**
         * The final status of the POJO publish attempt.
         */
        private final Status status;

        /**
         * A list of any errors that occurred during the POJO publish attempt.
         */
        private final List<Error> errors;

        @JsonCreator
        public PublishAttempt(@JsonProperty("timestamp") long timestamp, @JsonProperty("cause") Trigger trigger, @JsonProperty("status") Status status,
                        @JsonProperty("errors") List<Error> errors) {
            this.timestamp = timestamp;
            this.trigger = trigger;
            this.status = status;
            this.errors = errors == null ? List.of() : List.copyOf(errors);
        }

        public long getTimestamp() {
            return timestamp;
        }

        public Trigger getCause() {
            return trigger;
        }

        public Status getStatus() {
            return status;
        }

        public List<Error> getErrors() {
            return errors;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PublishAttempt that = (PublishAttempt) o;
            return trigger == that.trigger && status == that.status && Objects.equals(timestamp, that.timestamp) && Objects.equals(errors, that.errors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(trigger, status, timestamp, errors);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", PublishAttempt.class.getSimpleName() + "[", "]").add("time=" + timestamp).add("cause=" + trigger)
                            .add("status=" + status).add("errors=" + errors).toString();
        }
    }

}
