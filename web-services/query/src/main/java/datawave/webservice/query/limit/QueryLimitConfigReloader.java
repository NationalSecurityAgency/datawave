package datawave.webservice.query.limit;

import static org.apache.commons.lang.StringUtils.split;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.format.DataFormatDetector;
import com.fasterxml.jackson.core.format.DataFormatMatcher;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

/**
 * This class provides functionality for leveraging Zookeeper to watch for changes that should trigger a reload of a {@link QueryLimitConfiguration} and provide
 * it to listeners. It is expected that only a singleton {@link QueryLimitConfigReloader} will exist to be injected where needed. The Zookeeper logic here
 * requires that only a singleton {@link QueryLimitConfigReloader} is used on each server.
 */
public class QueryLimitConfigReloader implements AutoCloseable {

    public static final String ZOOKEEPER_NAMESPACE = "QueryLimitConfig";

    private static final Logger log = Logger.getLogger(QueryLimitConfigReloader.class);

    private static final String NODE_PATH = "/path";
    private static final String NODE_TRIGGER = "/trigger";
    private static final String NODE_ATTEMPTS = "/attempts";
    private static final String NODE_CAUSE = "/cause";
    private static final String NODE_STATUS = "/status";
    private static final String NODE_ERRORS = "/errors";
    private static final String NODE_ERROR_BASE = "/error_";
    private static final String NODE_TIME = "/time";

    /**
     * Mapper for JSON files.
     */
    private static final JsonMapper jsonMapper = JsonMapper.builder().build();

    /**
     * Mapper for XML files.
     */
    private static final XmlMapper xmlMapper = XmlMapper.builder().build();

    /**
     * Mapper for YAML files.
     */
    private static final YAMLMapper yamlMapper = YAMLMapper.builder().build();

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
     * The list of listeners that should be supplied with new {@link QueryLimitConfiguration} after successful reloads.
     */
    private List<Consumer<QueryLimitConfiguration>> listeners = new CopyOnWriteArrayList<>();

    /**
     * A {@link CuratorCache} that will listen for creates and modifications of the node {@code /path}.
     */
    private CuratorCache pathCache;

    /**
     * A {@link CuratorCache} that will listen for creates, modifications, and deletions of the node {@code trigger}
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
     * The lock that must be obtained by any task calling {@link #triggerReload(ReloadCause)} in order to perform a reload.
     */
    private final Lock reloadLock = new ReentrantLock();

    /**
     * An executor that runs 1 task, and keeps at most 1 in the queue. If a 3rd task arrives, the one in the queue is discarded for the new one. If a bunch of
     * reloads occur, we are only interested in supplying listeners with the latest reload attempt.
     */
    // @formatter:off
    private ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    1, // Use a core pool size of 1.
                    1, // The maximum pool size is 1.
                    0L, TimeUnit.MILLISECONDS, // Keep alive time of 1 ms for idle threads.
                    new ArrayBlockingQueue<>(1), // Only allow 1 task to be queued at a time.
                    new ThreadPoolExecutor.DiscardOldestPolicy()); // If a new task is submitted, discard any task present in the queue.
    // @formatter:on

    /**
     * The client dispatcher.
     */
    private LockedZkClientDispatcher clientDispatcher;

    /**
     * The finalized path for the node {@code /attempts/<serverIpAddress>}.
     */
    private String baseAttemptNode;

    /**
     * The finalized path for the node {@code /attempts/<serverIpAddress>/cause}.
     */
    private String attemptCauseNode;

    /**
     * The finalized path for the node {@code /attempts/<serverIpAddress>/status}.
     */
    private String attemptStatusNode;

    /**
     * The finalized path for the node {@code /attempts/<serverIpAddress>/errors}.
     */
    private String attemptErrorsNode;

    /**
     * The finalized path for the node {@code /attempts/<serverIpAddress>/time}.
     */
    private String attemptTimeNode;

    /**
     * The configuration to use when connecting to HDFS.
     */
    private Configuration hadoopConfig;

    /**
     * The zookeeper config.
     */
    private String zookeeperConfig;

    /**
     * The HDFS site config URLs.
     */
    private String hdfsConfigUrls;

    /**
     * Indicates whether a reload attempt succeeded for failed.
     */
    public enum ReloadStatus {
        /**
         * Indicates a reload attempt was successful.
         */
        SUCCESS,
        /**
         * Indicates a new {@link QueryLimitConfigReloader} could not be loaded from Zookeeper.
         */
        RELOAD_ERROR,
        /**
         * Indicates a new {@link QueryLimitConfigReloader} was successfully loaded from Zookeeper, but an error occurred when supplying it to a listener.
         */
        LISTENER_ERROR
    }

    /**
     * Indicates the triggering event that launched a new reload attempt.
     */
    public enum ReloadCause {
        /**
         * Indicates the triggering event was the creation of the node {@value #NODE_PATH} with non-empty data.
         */
        PATH_NODE_CREATED,
        /**
         * Indicates the triggering event was the modification of the node {@value #NODE_PATH} with non-empty data.
         */
        PATH_NODE_MODIFIED,
        /**
         * Indicates the triggering event was the creation of the node {@value #NODE_TRIGGER}.
         */
        TRIGGER_NODE_CREATED,
        /**
         * Indicates the triggering event was the modification of the node {@value #NODE_TRIGGER}.
         */
        TRIGGER_NODE_MODIFIED,
        /**
         * Indicates the triggering event was the deletion of the node {@value #NODE_TRIGGER}.
         */
        TRIGGER_NODE_DELETED
    }

    /**
     * Return the zookeeper configs.
     *
     * @return the zookeeper config
     */
    public String getZookeeperConfig() {
        return zookeeperConfig;
    }

    /**
     * Set the zookeeper configs. This can be a comma-delimited list of zookeeper hosts or a path to a local zookeeper config file.
     */
    public void setZookeeperConfig(String zookeeperConfig) {
        this.zookeeperConfig = zookeeperConfig;
    }

    /**
     * Return the HDFS site config URLs.
     *
     * @return the URLs
     */
    public String getHdfsConfigUrls() {
        return hdfsConfigUrls;
    }

    /**
     * Set a comma-delimited list of HDFS configuration files.
     *
     * @param hdfsConfigUrls
     *            the URLs
     */
    public void setHdfsConfigUrls(String hdfsConfigUrls) {
        this.hdfsConfigUrls = hdfsConfigUrls;
    }

    /**
     * Create the underlying {@link CuratorCache} caches that will watch for changes to the nodes {@code /path} and {@code /trigger}. These caches require some
     * backend initialization before they can start listening for node changes. Use {@link QueryLimitConfigReloader#awaitCacheInitialization(long, TimeUnit)} to
     * await the cache initialization. For testing purposes, this method should be called after setting the zookeeper configs, hdfs site config URLs, and client
     * cleanup interval.
     */
    public void setup() throws QuorumPeerConfig.ConfigException {
        // If the zookeeper config points to a file, extract the hosts from it.
        this.zookeeperConfig = ZookeeperUtils.getQuorumPeerConfig(this.zookeeperConfig);

        // @formatter:off
        CuratorFrameworkFactory.Builder clientFactory = CuratorFrameworkFactory.builder()
                        .namespace(ZOOKEEPER_NAMESPACE)
                        .connectString(zookeeperConfig)
                        .sessionTimeoutMs(60000)
                        .connectionTimeoutMs(60000)
                        .retryPolicy(new RetryNTimes(10, 1000));
        // @formatter:on

        clientDispatcher = new LockedZkClientDispatcher(clientFactory, 120000, 120000, TimeUnit.MILLISECONDS);
        this.pathCache = createCache(NODE_PATH, clientFactory, () -> createPathCacheListener(pathCacheInitialized));
        this.triggerCache = createCache(NODE_TRIGGER, clientFactory, () -> createTriggerCacheListener(triggerCacheInitialized));
        try {
            this.hadoopConfig = new Configuration();
            if (hdfsConfigUrls != null && !hdfsConfigUrls.isBlank()) {
                for (String url : split(hdfsConfigUrls, ',')) {
                    hadoopConfig.addResource(new URL(url));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load hadoop configuration from URLs '" + hdfsConfigUrls + "'", e);
        }

        // Construct the finalized attempt node paths to be relative to the server IP address.
        try {
            String serverIpAddress = InetAddress.getLocalHost().getHostAddress();
            baseAttemptNode = NODE_ATTEMPTS + "/" + serverIpAddress;
            attemptCauseNode = baseAttemptNode + NODE_CAUSE;
            attemptStatusNode = baseAttemptNode + NODE_STATUS;
            attemptErrorsNode = baseAttemptNode + NODE_ERRORS;
            attemptTimeNode = baseAttemptNode + NODE_TIME;
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to get local host address", e);
        }
    }

    public void shutdown() {
        close();
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
    private CuratorCache createCache(String node, CuratorFrameworkFactory.Builder clientFactory, Supplier<CuratorCacheListener> listenerSupplier) {
        try {
            CuratorFramework client = clientFactory.build();
            client.start();
            CuratorCache cache = CuratorCache.build(client, node, CuratorCache.Options.SINGLE_NODE_CACHE);
            // Add the desired listeners to the cache.
            CuratorCacheListener cacheListener = listenerSupplier.get();
            cache.listenable().addListener(cacheListener);
            // Start the cache.
            cache.start();
            return cache;
        } catch (Exception e) {
            log.error("Failed to create curator cache for path node " + node, e);
            throw new RuntimeException("Failed to create curator cache for path " + node, e);
        }
    }

    /**
     * Create and return a {@link CuratorCacheListener} that will listen for creations and modifications of the node {@code /path}, and trigger a configuration
     * reload if the updated {@code /path} node has non-empty data. The listener will also set the given boolean to true when its wrapping {@link CuratorCache}
     * is initialized.
     *
     * @param initFlag
     *            a flag to set to true when an initialized event is received by the listener
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
                        if(log.isDebugEnabled()) {
                            log.debug("Triggering reload due to creation of node " + NODE_PATH + " with non-empty data at time " + System.currentTimeMillis());
                        }
                        executor.submit(()-> triggerReload(ReloadCause.PATH_NODE_CREATED));
                    }
                })
                .forChanges((oldNode, newNode) -> {
                    byte[] newData = newNode.getData();
                    // Only trigger a reload attempt if the data is not empty.
                    if(newData != null && newData.length > 0) {
                        if(log.isDebugEnabled()){
                            log.debug("Triggering reload due to modification of node " + NODE_PATH + " with non-empty data");
                        }
                        executor.submit(()-> triggerReload(ReloadCause.PATH_NODE_MODIFIED));
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
                .forCreates((node) -> {
                    if(log.isDebugEnabled()){
                        log.debug("Trigger reload due to creation of node " + NODE_TRIGGER );
                    }
                    executor.submit(()-> triggerReload(ReloadCause.TRIGGER_NODE_CREATED));
                })
                .forChanges((oldNode, newNode) -> {
                    if(log.isDebugEnabled()){
                        log.debug("Triggering reload due to modification of node " + NODE_TRIGGER);
                    }
                    executor.submit(() -> triggerReload(ReloadCause.TRIGGER_NODE_MODIFIED));
                })
                .forDeletes((node) -> {
                    if(log.isDebugEnabled()){
                        log.debug("Triggering reload due to deletion of node " + NODE_TRIGGER);
                    }
                    executor.submit(() -> triggerReload(ReloadCause.TRIGGER_NODE_DELETED));
                })
                .build();
        // @formatter:on
    }

    /**
     * Wait until the underlying node caches are initialized for at most the given timeout.
     *
     * @param timeout
     *            the timeout
     * @param unit
     *            the time unit
     * @throws ConditionTimeoutException
     *             if the caches are not initialized before the timeout
     */
    public void awaitCacheInitialization(long timeout, TimeUnit unit) throws ConditionTimeoutException {
        Awaitility.await().atMost(timeout, unit).until(this::areCachesInitialized);
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
     * Trigger a configuration reload. If a valid configuration is loaded
     */
    private void triggerReload(ReloadCause cause) {
        if (log.isDebugEnabled()) {
            log.debug("Configuration reload triggered");
        }

        // Obtain the reload lock.
        reloadLock.lock();
        try {
            Instant attemptTime = Instant.now();
            // Attempt to load the configuration from the path node.
            LoadResult result = loadConfiguration();

            // If we successfully loaded a valid configuration, pass it to any listeners registered with this loader.
            if (result.status == ReloadStatus.SUCCESS) {
                if (!listeners.isEmpty()) {
                    for (Consumer<QueryLimitConfiguration> listener : listeners) {
                        try {
                            listener.accept(result.config);
                        } catch (Exception e) {
                            // If an exception is thrown by a listener, log it and record it in the status.
                            log.warn("Exception thrown by listener " + listener, e);
                            result.setStatus(ReloadStatus.LISTENER_ERROR);
                            result.addErrorMessage("Exception thrown by listener: " + e.getMessage());
                        }
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Supplied configuration update to all listeners");
                    }
                } else {
                    log.debug("No listeners registered to be supplied configuration updates");
                }
            }

            // Update the attempt nodes for the latest attempt.
            updateAttemptNodes(cause, result.getStatus(), result.getErrorMessages(), attemptTime);
        } catch (Exception e) {
            log.error("Failed to reload configuration", e);
            throw new RuntimeException("Failed to reload configuration", e);
        } finally {
            reloadLock.unlock();
        }

        log.debug("Reload complete");
    }

    /**
     * Make the following changes underneath the namespace {@value ZOOKEEPER_NAMESPACE}. All nodes listed here will be created if they do not exist:
     * <ul>
     * <li>Set the data for the node {@code /attempts/<serverIpAddress>/status} to the bytes of the string form of the given {@link ReloadStatus}.</li>
     * <li>Set the data for the node {@code /attempts/<serverIpAddress>/cause} to the bytes of the string form of the given {@link ReloadCause}.</li>
     * <li>Set the data for the node {@code /attempts/<serverIpAddress>/time} to the bytes of the string form of the given {@link Instant}.</li>
     * <li>Depending on the list of error messages provided, make the following changes to {@code /attempts/<serverIpAddress>/errors}:
     * <ul>
     * <li>If the error messages list is empty, delete the node {@code /attempts/<serverIdAddress>/errors} and its children.</li>
     * <li>If the error messages list is not empty, set the children of the node {@code /attempts/<serverIpAddress>/errors} such that there is one child for
     * each error message, with the path {@code error_X} where X equals the index of the error message in the list, and data is set to the bytes of the error
     * message.</li>
     * <ul/>
     * </li>
     * </ul>
     *
     * @param cause
     *            the triggering event for the reload
     * @param status
     *            the status
     * @param errorMessages
     *            the error messages
     * @param time
     *            the time of the attempt
     * @throws Exception
     *             if an error occurs on Zookeeper
     */
    private void updateAttemptNodes(ReloadCause cause, ReloadStatus status, List<String> errorMessages, Instant time) throws Exception {
        try (LockedZkClientDispatcher.LockedClient lockedClient = clientDispatcher.getLockedClient()) {
            CuratorFramework client = lockedClient.getClient();
            // Ensure the base /reload node is created.
            client.createContainers(baseAttemptNode);

            setData(client, attemptCauseNode, cause.toString().getBytes());
            setData(client, attemptStatusNode, status.toString().getBytes());
            updateErrorsNode(client, errorMessages);
            setData(client, attemptTimeNode, time.toString().getBytes());
        }
    }

    /**
     * Update the node {@code /attempts/<serverIpAddress>/errors} to reflect the contents of the given error message list.
     *
     * @param client
     *            the client
     * @param errorMessages
     *            the error messages
     * @throws Exception
     *             if an error occurs in Zookeeper
     */
    private void updateErrorsNode(CuratorFramework client, List<String> errorMessages) throws Exception {
        Stat stat = client.checkExists().forPath(attemptErrorsNode);
        if (stat != null) {
            client.delete().deletingChildrenIfNeeded().forPath(attemptErrorsNode);
        }
        if (!errorMessages.isEmpty()) {
            client.create().forPath(attemptErrorsNode);
            for (int i = 0; i < errorMessages.size(); i++) {
                String messageNode = attemptErrorsNode + NODE_ERROR_BASE + i;
                setData(client, messageNode, errorMessages.get(i).getBytes());
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
     * Attempt to load a {@link QueryLimitConfiguration} from the path specified in the data of the node {@value NODE_PATH} under the zookeeper namespace
     * {@value ZOOKEEPER_NAMESPACE}. The path may point to an http, hdfs, or local file. Note that an invocation of this method will not result in the
     * configuration being supplied to any listeners.
     *
     * @return the reload result
     */
    public LoadResult loadConfiguration() {
        if (log.isDebugEnabled()) {
            log.debug("Attempting to load new query limit configuration");
        }
        try (LockedZkClientDispatcher.LockedClient lockedClient = clientDispatcher.getLockedClient()) {
            CuratorFramework client = lockedClient.getClient();

            // Verify that the config URL node exists.
            Stat stat = client.checkExists().forPath(NODE_PATH);
            if (stat == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Node " + NODE_PATH + " does not exist, skipping reload");
                }
                return LoadResult.reloadError("Node does not exist: " + NODE_PATH);
            }

            // Fetch the path from the path node.
            byte[] pathBytes = client.getData().forPath(NODE_PATH);

            // Verify we have a non-blank path.
            if (pathBytes == null || pathBytes.length == 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Node " + NODE_PATH + " does not have a non-blank filepath, skipping reload");
                }
                return LoadResult.reloadError("Config file path is not set in data for node " + NODE_PATH);
            }

            String path = new String(pathBytes);
            if (path.isBlank()) {
                if (log.isDebugEnabled()) {
                    log.debug("Blank config filepath set in data for node " + NODE_PATH + ", skipping reload");
                }
                return LoadResult.reloadError("Config file path is not set in data for node " + NODE_PATH);
            }

            // Trim the path of any leading/trailing whitespace.
            path = path.trim();

            // Read the contents of the file.
            byte[] contents;
            try {
                contents = getFileContents(path);
            } catch (NoSuchFileException e) {
                log.error("Failed to read contents from file " + path, e);
                return LoadResult.reloadError("File not found: " + path);
            } catch (Exception e) {
                log.error("Failed to read contents from file " + path, e);
                return LoadResult.reloadError("Failed to read contents from file " + path + ": " + e.getMessage());
            }

            // Determine the format (XML, JSON, YAML) and use the corresponding mapper to deserialize the contents.
            QueryLimitConfiguration config;
            DataFormatMatcher format = formatDetector.findFormat(contents);
            if (format.hasMatch()) {
                JsonFactory factory = format.getMatch();
                if (log.isDebugEnabled()) {
                    log.debug("Deserializing config file using format " + factory.getFormatName());
                }
                try {
                    // Deserialize the configuration using the associated mapper for the format.
                    config = formatToMapper.get(factory.getFormatName()).readValue(contents, QueryLimitConfiguration.class);
                } catch (Exception e) {
                    log.error("Failed to deserialize file " + path + " to a " + QueryLimitConfiguration.class.getName(), e);
                    return LoadResult.reloadError("Failed to deserialize file to a " + QueryLimitConfiguration.class.getSimpleName());
                }
            } else {
                // If we do not have a match for a supported mapper, return an error.
                if (log.isDebugEnabled()) {
                    log.debug("Query limit file " + path + " is not XML, JSON, or YAML, skipping reload");
                }
                return LoadResult.reloadError("Config file must be XML, JSON, or YAML");
            }

            // If we successfully deserialize a QueryLimitConfiguration, validate it.
            try {
                QueryLimitConfigurationValidator.validate(config);
                if (log.isDebugEnabled()) {
                    log.debug("Successfully loaded query limit configuration from file " + path + ": " + config);
                }
                return LoadResult.success(config);
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("Query limit configuration failed validation, skipping reload", e);
                }
                return LoadResult.reloadError("Configuration failed validation: " + e.getMessage());
            }
        } catch (Exception e) {
            log.error("Failed to load query limit configuration from Zookeeper nodes", e);
            throw new RuntimeException("Failed to load query limit configuration from Zookeeper nodes", e);
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
                throw new IOException("Unsupported URI scheme '" + scheme + "'");
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
            log.debug("Attempting to load query limit configuration from URL: " + path);
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
            log.debug("Attempting to load query limit configuration from HDFS file: " + path);
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
            log.debug("Attempting to load query limit configuration from local file: " + path);
        }
        try (InputStream is = Files.newInputStream(Path.of(path), StandardOpenOption.READ)) {
            return IOUtils.toByteArray(is);
        }
    }

    /**
     * Add a {@link Consumer} that, when a new {@link QueryLimitConfiguration} is loaded a path specified in Zookeeper, will be provided that configuration.
     *
     * @param listener
     *            the listener
     */
    public void addListener(Consumer<QueryLimitConfiguration> listener) {
        this.listeners.add(listener);
    }

    /**
     * Perform the following tasks:
     * <ul>
     * <li>Close the curator caches for the nodes {@value #NODE_PATH} and @value #NODE_TRIGGER}.</li>
     * <li>Shut down the executor service that executes reload tasks.</li>
     * <li>Clear the listener list.</li>
     * <li>Close the locked client dispatcher.</li>
     * </ul>
     */
    public void cleanup() {
        if (pathCache != null) {
            try {
                pathCache.close();
            } catch (Exception e) {
                log.error("Failed to close path cache", e);
            }
            pathCache = null;
        }
        if (triggerCache != null) {
            try {
                triggerCache.close();
            } catch (Exception e) {
                log.error("Failed to close trigger cache", e);
            }
            triggerCache = null;
        }
        if (executor != null) {
            try {
                executor.shutdown();
            } catch (Exception e) {
                log.error("Failed to close executor", e);
            }
            executor = null;
        }

        if (listeners != null) {
            try {
                listeners.clear();
            } catch (Exception e) {
                log.error("Failed to clear listeners", e);
            } finally {
                listeners = null;
            }
        }

        if (clientDispatcher != null) {
            try {
                clientDispatcher.close();
            } catch (Exception e) {
                log.error("Failed to close client dispatcher", e);
            }
            clientDispatcher = null;
        }
    }

    /**
     * Clean up resources used by this {@link QueryLimitConfigReloader} via {@link #cleanup()}.
     */
    @Override
    public void close() {
        cleanup();
    }

    public static class LoadResult {
        private final QueryLimitConfiguration config;
        private final List<String> errorMessages = new ArrayList<>();
        private ReloadStatus status;

        public static LoadResult success(QueryLimitConfiguration config) {
            return new LoadResult(config, ReloadStatus.SUCCESS);
        }

        public static LoadResult reloadError(String message) {
            LoadResult result = new LoadResult(null, ReloadStatus.RELOAD_ERROR);
            result.addErrorMessage(message);
            return result;
        }

        private LoadResult(QueryLimitConfiguration config, ReloadStatus status) {
            this.config = config;
            this.status = status;
        }

        public QueryLimitConfiguration getConfig() {
            return config;
        }

        public ReloadStatus getStatus() {
            return status;
        }

        public void setStatus(ReloadStatus status) {
            this.status = status;
        }

        public void addErrorMessage(String message) {
            errorMessages.add(message);
        }

        public List<String> getErrorMessages() {
            return errorMessages;
        }
    }
}
