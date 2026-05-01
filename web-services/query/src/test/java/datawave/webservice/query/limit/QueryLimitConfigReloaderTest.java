package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueryLimitConfigReloaderTest {

    private static String validJsonFile;
    private static String validXmlFile;
    private static String validYamlFile;
    private static String invalidConfigFile;
    private static String nonConfigFile;
    private static String unsupportedFormatFile;

    private QueryLimitConfigReloader reloader;
    private final List<QueryLimitConfiguration> configs = new ArrayList<>();
    private TestingServer server;
    private CuratorFramework client;

    private static String causeNode;
    private static String statusNode;
    private static String errorsNode;
    private static String timeNode;

    private Instant testStartTime;

    @BeforeAll
    static void beforeAll() throws Exception {
        String serverIpAddress = InetAddress.getLocalHost().getHostAddress();
        causeNode = "/attempts/" + serverIpAddress + "/cause";
        statusNode = "/attempts/" + serverIpAddress + "/status";
        errorsNode = "/attempts/" + serverIpAddress + "/errors";
        timeNode = "/attempts/" + serverIpAddress + "/time";

        ClassLoader classLoader = QueryLimitConfigReloaderTest.class.getClassLoader();
        validJsonFile = getAbsolutePath(classLoader, "queryLimits/valid_config.json");
        validXmlFile = getAbsolutePath(classLoader, "queryLimits/valid_config.xml");
        validYamlFile = getAbsolutePath(classLoader, "queryLimits/valid_config.yaml");
        invalidConfigFile = getAbsolutePath(classLoader, "queryLimits/invalid_config.yaml");
        nonConfigFile = getAbsolutePath(classLoader, "queryLimits/non_config.yaml");
        unsupportedFormatFile = getAbsolutePath(classLoader, "queryLimits/unsupported_format.toml");
    }

    /**
     * Return the absolute path for the given file as resolved by the classloader.
     *
     * @param classLoader
     *            the classloader
     * @param relativePath
     *            the relative path
     * @return the absolute path
     * @throws URISyntaxException
     *             if the URL cannot be converted to a URI
     */
    private static String getAbsolutePath(ClassLoader classLoader, String relativePath) throws URISyntaxException {
        URL url = classLoader.getResource(relativePath);
        if (url != null) {
            return Paths.get(url.toURI()).toAbsolutePath().toString();
        } else {
            throw new NullPointerException("Null URL returned for relative path '" + relativePath);
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        testStartTime = Instant.now();
        configs.clear();
        reloader = null;
        server = new TestingServer();
        server.start();
        // @formatter:off
        client = CuratorFrameworkFactory.builder().namespace("QueryLimitConfig")
                        .connectString(server.getConnectString())
                        .sessionTimeoutMs(300000)
                        .connectionTimeoutMs(60000)
                        .retryPolicy(new RetryNTimes(10, 1000))
                        .build();
        // @formatter:on
        client.start();

    }

    @AfterEach
    void tearDown() throws IOException {
        if (reloader != null) {
            reloader.close();
        }
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    /**
     * Verify that the {@link QueryLimitConfigReloader} can read a {@link QueryLimitConfiguration} from a JSON file on the local file system.
     */
    @Test
    void testReloadValidJsonFromLocalFilesystem() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validJsonFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Wait for the configuration to be loaded.
        waitForConfigurationToBeSupplied();

        // Verify that a single configuration was supplied to the listener.
        assertEquals(1, configs.size());

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that the {@link QueryLimitConfigReloader} can read a {@link QueryLimitConfiguration} from an XML file on the local file system.
     */
    @Test
    void testReloadValidXmlFromLocalFilesystem() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validXmlFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Wait for the configuration to be loaded.
        waitForConfigurationToBeSupplied();

        // Verify that a single configuration was supplied to the listener.
        assertEquals(1, configs.size());

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that the {@link QueryLimitConfigReloader} can read a {@link QueryLimitConfiguration} from a YAML file on the local file system.
     */
    @Test
    void testReloadValidYamlFromLocalFilesystem() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validYamlFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Wait for the configuration to be loaded.
        waitForConfigurationToBeSupplied();

        // Verify that a single configuration was supplied to the listener.
        assertEquals(1, configs.size());

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that if the file path is prefixed with {@code file://}, {@link QueryLimitConfigReloader} will attempt to read the file from the local file system.
     */
    @Test
    void testFilePrefixWillReloadFromLocalFilesystem() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", "file://" + validJsonFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Wait for the configuration to be loaded.
        waitForConfigurationToBeSupplied();

        // Verify that a single configuration was supplied to the listener.
        assertEquals(1, configs.size());

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that if the node {@code /trigger} is created, {@link QueryLimitConfigReloader} will reload the configuration.
     */
    @Test
    void testReloadTriggeredByTriggerNodeCreation() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validJsonFile);

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Wait for the configuration to be loaded.
        waitForConfigurationToBeSupplied();

        // Verify that a single configuration was supplied to the listener.
        assertEquals(1, configs.size());

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_CREATED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that if the node {@code /trigger} is deleted, {@link QueryLimitConfigReloader} will reload the configuration.
     */
    @Test
    void testReloadTriggeredByTriggerNodeDeleted() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validJsonFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        client.delete().forPath("/trigger");

        // Wait for the configuration to be loaded.
        waitForConfigurationToBeSupplied();

        // Verify that a single configuration was supplied to the listener.
        assertEquals(1, configs.size());

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_DELETED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that if the node {@code /path} is created with empty data, {@link QueryLimitConfigReloader} will not reload a configuration.
     */
    @Test
    void testReloadNotTriggeredByPathNodeCreationWithEmptyData() throws Exception {
        // Create the reloader and start listening for trigger events.
        createReloader();

        // Create the /path node.
        createOrUpdateNode("/path", "");

        // Verify a configuration is never supplied to the listener.
        assertThrows(Exception.class, () -> Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> !configs.isEmpty()));

        // Verify the status nodes are never created.
        assertNull(client.checkExists().forPath(causeNode));
        assertNull(client.checkExists().forPath(statusNode));
        assertNull(client.checkExists().forPath(errorsNode));
        assertNull(client.checkExists().forPath(timeNode));
    }

    /**
     * Verify that if the node {@code /path} is modified with empty data, {@link QueryLimitConfigReloader} will not reload a configuration.
     */
    @Test
    void testReloadNotTriggeredByPathNodeModificationWithEmptyData() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validJsonFile);

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Modify the /path node.
        createOrUpdateNode("/path", "");

        // Verify a configuration is never supplied to the listener.
        assertThrows(Exception.class, () -> Awaitility.await().atMost(1, TimeUnit.SECONDS).until(() -> !configs.isEmpty()));

        // Verify the status nodes are never created.
        assertNull(client.checkExists().forPath(causeNode));
        assertNull(client.checkExists().forPath(statusNode));
        assertNull(client.checkExists().forPath(errorsNode));
        assertNull(client.checkExists().forPath(timeNode));
    }

    /**
     * Verify that if the node {@code /path} is created with non-empty data, {@link QueryLimitConfigReloader} will reload a configuration.
     */
    @Test
    void testReloadTriggeredByPathNodeCreationWithNonEmptyData() throws Exception {
        // Create the reloader and start listening for trigger events.
        createReloader();

        // Create the /path node.
        createOrUpdateNode("/path", validJsonFile);

        // Wait for the configuration to be loaded.
        waitForConfigurationToBeSupplied();

        // Verify that a single configuration was supplied to the listener.
        assertEquals(1, configs.size());

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.PATH_NODE_CREATED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that if the node {@code /path} is modified with non-empty data, {@link QueryLimitConfigReloader} will reload a configuration.
     */
    @Test
    void testReloadTriggeredByPathModificationWithNonEmptyData() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validJsonFile);

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Modify the /path node.
        createOrUpdateNode("/path", validXmlFile);

        // Wait for the configuration to be loaded.
        waitForConfigurationToBeSupplied();

        // Verify that a single configuration was supplied to the listener.
        assertEquals(1, configs.size());

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.PATH_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify we do not load a configuration when the node {@code /path} does not exist.
     */
    @Test
    void testNonExistentPathNode() throws Exception {
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Verify a configuration is never supplied to the listener.
        waitToCheckConfigurationIsNotSupplied();

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.RELOAD_ERROR);
        assertErrors("Node does not exist: /path");
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify we do not load a configuration when the node {@code /path} has an empty filepath.
     */
    @Test
    void testPathNodeWithEmptyFilepath() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", null);
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Verify a configuration is never supplied to the listener.
        waitToCheckConfigurationIsNotSupplied();

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.RELOAD_ERROR);
        assertErrors("Config file path is not set in data for node /path");
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify we do not load a configuration when the node {@code /path} has a blank filepath.
     */
    @Test
    void testPathNodeWithBlankFilepath() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", "   ");
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Verify a configuration is never supplied to the listener.
        waitToCheckConfigurationIsNotSupplied();

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.RELOAD_ERROR);
        assertErrors("Config file path is not set in data for node /path");
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify we do not load a configuration when the node {@code /path} has a filepath with an unsupported URI scheme.
     */
    @Test
    void testFileWithInvalidURIScheme() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", "ftp://i/do/not/exist");
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Verify a configuration is never supplied to the listener.
        waitToCheckConfigurationIsNotSupplied();

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.RELOAD_ERROR);
        assertErrors("Failed to read contents from file ftp://i/do/not/exist: Unsupported URI scheme 'ftp'");
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify we do not load a configuration when the node {@code /path} has a filepath for a file that does not exist.
     */
    @Test
    void testNonExistentFile() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", "i/do/not/exist");
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Verify a configuration is never supplied to the listener.
        waitToCheckConfigurationIsNotSupplied();

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.RELOAD_ERROR);
        assertErrors("File not found: i/do/not/exist");
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify we do not load a configuration when the node {@code /path} points to a file with unsupported syntax.
     */
    @Test
    void testUnsupportedSyntax() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", unsupportedFormatFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Verify a configuration is never supplied to the listener.
        waitToCheckConfigurationIsNotSupplied();

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.RELOAD_ERROR);
        assertErrors("Config file must be XML, JSON, or YAML");
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify we do not load a configuration when the node {@code /path} points to a file that cannot be deserialized as a {@link QueryLimitConfiguration}.
     */
    @Test
    void testNonQueryLimitConfigurationFile() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", nonConfigFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Verify a configuration is never supplied to the listener.
        waitToCheckConfigurationIsNotSupplied();

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.RELOAD_ERROR);
        assertErrors("Failed to deserialize file to a QueryLimitConfiguration");
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify we do not load a configuration when the node {@code /path} points to a file that deserializes to a {@link QueryLimitConfiguration} that fails
     * validation.
     */
    @Test
    void testInvalidQueryLimitConfigurationFile() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", invalidConfigFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Verify a configuration is never supplied to the listener.
        waitToCheckConfigurationIsNotSupplied();

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.RELOAD_ERROR);
        assertErrors("Configuration failed validation: Default user query limit must be greater than 0");
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that if exceptions are thrown by listeners after supplying them with a new configuration, the errors are captured and recorded.
     */
    @Test
    void testExceptionsThrownByListeners() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validJsonFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the reloader and start listening for trigger events.
        createReloader();

        // Add listeners to the reloader that will throw a variety of exceptions.
        reloader.addListener(configuration -> {
            throw new NullPointerException("Something bad happened!");
        });
        reloader.addListener(configuration -> {
            throw new IllegalArgumentException("I don't like this configuration.");
        });
        reloader.addListener(configuration -> {
            throw new UnsupportedOperationException("Why do I even exist?");
        });

        // Trigger a reload.
        createOrUpdateNode("/trigger", "");

        // Wait for the configuration to be loaded.
        waitForConfigurationToBeSupplied();

        // Verify that a single configuration was supplied to the listener that does not throw an exception.
        assertEquals(1, configs.size());

        // Wait for the attempt time node to be created. This is the last attempt node that is created/updated after a reload and is our indicator that we can
        // verify the attempt nodes.
        waitForAttemptTimeNodeToBeCreated();

        // Verify the attempt nodes were updated correctly.
        assertCause(QueryLimitConfigReloader.ReloadCause.TRIGGER_NODE_MODIFIED);
        assertStatus(QueryLimitConfigReloader.ReloadStatus.LISTENER_ERROR);
        assertErrors("Exception thrown by listener: Something bad happened!", "Exception thrown by listener: I don't like this configuration.",
                        "Exception thrown by listener: Why do I even exist?");
        assertTimeNodeHasRecentTime();
    }

    /**
     * Wait with a timeout until we
     */
    private void waitForConfigurationToBeSupplied() {
        try {
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !configs.isEmpty());
        } catch (Exception e) {
            fail("Timeout exceeded while waiting for config to be loaded: " + e.getMessage());
        }
    }

    /**
     * Wait a length of time and fail if we are supplied a configuration in that time period. If this check fails sometimes, increase the timeout.
     */
    private void waitToCheckConfigurationIsNotSupplied() {
        assertThrows(Exception.class, () -> Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> !configs.isEmpty()),
                        "Expected configuration to never be supplied to listener");
    }

    /**
     * Wait with a timeout until we know the node {@code attempts/<serverIpAddress>/time} exists.
     */
    private void waitForAttemptTimeNodeToBeCreated() {
        try {
            Awaitility.await().atMost(4, TimeUnit.SECONDS).until(() -> client.checkExists().forPath(timeNode) != null);
        } catch (Exception e) {
            fail("Timeout exceeded while waiting for node " + timeNode + " to be created: " + e.getMessage());
        }
    }

    private void assertCause(QueryLimitConfigReloader.ReloadCause cause) throws Exception {
        assertData(causeNode, cause.toString());
    }

    private void assertStatus(QueryLimitConfigReloader.ReloadStatus status) throws Exception {
        assertData(statusNode, status.toString());
    }

    private void assertErrorsNodeDoesNotExist() throws Exception {
        Stat stat = client.checkExists().forPath(errorsNode);
        assertNull(stat, "Expected node " + errorsNode + " to not exist");
    }

    private void assertErrors(String... errors) throws Exception {
        Stat stat = client.checkExists().forPath(errorsNode);
        assertNotNull(stat, "Expected node " + errorsNode + " to exist");
        List<String> children = client.getChildren().forPath(errorsNode);
        List<String> actualErrors = new ArrayList<>();
        for (String child : children) {
            String actualData = new String(client.getData().forPath(errorsNode + "/" + child));
            actualErrors.add(actualData);
        }
        assertThat(actualErrors).containsExactlyInAnyOrder(errors);
    }

    private void assertTimeNodeHasRecentTime() throws Exception {
        Stat stat = client.checkExists().forPath(timeNode);
        assertNotNull(stat, "Expected node " + timeNode + " to exist");
        String actualData = new String(client.getData().forPath(timeNode));
        Instant timeNodeInstant = Instant.parse(actualData);
        assertTrue(timeNodeInstant.isAfter(testStartTime));
    }

    private void assertData(String path, String expectedData) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        assertNotNull(stat, "Expected node " + path + " to exist");
        String actualData = new String(client.getData().forPath(path));
        assertEquals(expectedData, actualData, "Expected data for node " + path + " to be '" + expectedData + "'");
    }

    private void createReloader() throws QuorumPeerConfig.ConfigException {
        reloader = new QueryLimitConfigReloader();
        reloader.setZookeeperConfig(server.getConnectString());
        reloader.setup();
        try {
            reloader.awaitCacheInitialization(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Reloader caches failed to initialize before timeout", e);
        }
        reloader.addListener(configs::add);
    }

    private void createOrUpdateNode(String node, String dataStr) throws Exception {
        Stat stat = client.checkExists().forPath(node);
        byte[] data = dataStr == null ? new byte[0] : dataStr.getBytes();
        if (stat == null) {
            client.create().forPath(node, data);
        } else {
            client.setData().forPath(node, data);
        }
    }
}
