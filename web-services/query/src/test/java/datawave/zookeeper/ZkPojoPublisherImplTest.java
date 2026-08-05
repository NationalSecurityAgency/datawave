package datawave.zookeeper;

import static datawave.zookeeper.ZkPojoPublisherImpl.PublishCause.PATH_NODE_CREATED;
import static datawave.zookeeper.ZkPojoPublisherImpl.PublishCause.PATH_NODE_MODIFIED;
import static datawave.zookeeper.ZkPojoPublisherImpl.PublishCause.TRIGGER_NODE_CREATED;
import static datawave.zookeeper.ZkPojoPublisherImpl.PublishCause.TRIGGER_NODE_DELETED;
import static datawave.zookeeper.ZkPojoPublisherImpl.PublishCause.TRIGGER_NODE_MODIFIED;
import static datawave.zookeeper.ZkPojoPublisherImpl.PublishStatus.*;
import static datawave.zookeeper.ZkPojoPublisherImpl.PublishStatus.LOAD_ERROR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

import datawave.ingest.util.ThreadUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.data.Stat;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.webservice.query.limit.QueryLimitConfiguration;

public class ZkPojoPublisherImplTest {

    private static final String NAMESPACE = "QueryLimitConfig";

    private static String validJsonFile;
    private static String validXmlFile;
    private static String validYamlFile;
    private static String invalidConfigFile;
    private static String nonConfigFile;
    private static String unsupportedFormatFile;

    private ZkPojoPublisherImpl publisher;
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

        ClassLoader classLoader = ZkPojoPublisherImplTest.class.getClassLoader();
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
        publisher = null;
        server = new TestingServer();
        server.start();
        // @formatter:off
        client = CuratorFrameworkFactory.builder().namespace(NAMESPACE)
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
        if (publisher != null) {
            publisher.close();
        }
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    /**
     * Verify that invalid constructor args will result in exceptions.
     */
    @Test
    void testInvalidConstructorArgs() {
        assertThatThrownBy(() -> new ZkPojoPublisherImpl(null, null, null)).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("zkClientBuilder must not be null");
        assertThatThrownBy(() -> new ZkPojoPublisherImpl(new ZkClientBuilder(), null, null))
                        .isInstanceOf(NullPointerException.class).hasMessage("objectClass must not be null");
    }

    /**
     * Verify that the {@link ZkPojoPublisherImpl} can read a {@link QueryLimitConfiguration} from a JSON file on the local file system.
     */
    @Test
    void testReloadValidJsonFromLocalFilesystem() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validJsonFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_MODIFIED);
        assertStatus(SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that the {@link ZkPojoPublisherImpl} can read a {@link QueryLimitConfiguration} from an XML file on the local file system.
     */
    @Test
    void testReloadValidXmlFromLocalFilesystem() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validXmlFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_MODIFIED);
        assertStatus(SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that the {@link ZkPojoPublisherImpl} can read a {@link QueryLimitConfiguration} from a YAML file on the local file system.
     */
    @Test
    void testReloadValidYamlFromLocalFilesystem() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validYamlFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_MODIFIED);
        assertStatus(SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that if the file path is prefixed with {@code file://}, {@link ZkPojoPublisherImpl} will attempt to read the file from the local file system.
     */
    @Test
    void testFilePrefixWillReloadFromLocalFilesystem() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", "file://" + validJsonFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_MODIFIED);
        assertStatus(SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that if the node {@code /trigger} is created, {@link ZkPojoPublisherImpl} will reload the configuration.
     */
    @Test
    void testReloadTriggeredByTriggerNodeCreation() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validJsonFile);

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_CREATED);
        assertStatus(SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that if the node {@code /trigger} is deleted, {@link ZkPojoPublisherImpl} will reload the configuration.
     */
    @Test
    void testReloadTriggeredByTriggerNodeDeleted() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validJsonFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_DELETED);
        assertStatus(SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that if the node {@code /path} is created with empty data, {@link ZkPojoPublisherImpl} will not reload a configuration.
     */
    @Test
    void testReloadNotTriggeredByPathNodeCreationWithEmptyData() throws Exception {
        // Create the publisher and start listening for trigger events.
        createPublisher();

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
     * Verify that if the node {@code /path} is modified with empty data, {@link ZkPojoPublisherImpl} will not reload a configuration.
     */
    @Test
    void testReloadNotTriggeredByPathNodeModificationWithEmptyData() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validJsonFile);

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
     * Verify that if the node {@code /path} is created with non-empty data, {@link ZkPojoPublisherImpl} will reload a configuration.
     */
    @Test
    void testReloadTriggeredByPathNodeCreationWithNonEmptyData() throws Exception {
        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(PATH_NODE_CREATED);
        assertStatus(SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that if the node {@code /path} is modified with non-empty data, {@link ZkPojoPublisherImpl} will reload a configuration.
     */
    @Test
    void testReloadTriggeredByPathModificationWithNonEmptyData() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validJsonFile);

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(PATH_NODE_MODIFIED);
        assertStatus(SUCCESS);
        assertErrorsNodeDoesNotExist();
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify we do not load a configuration when the node {@code /path} does not exist.
     */
    @Test
    void testNonExistentPathNode() throws Exception {
        createOrUpdateNode("/trigger", "changeme");

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_MODIFIED);
        assertStatus(LOAD_ERROR);
        assertTotalErrors(1);
        assertErrorMessage(0, "Node does not exist: QueryLimitConfig/path");
        assertErrorDoesNotHaveStackTrace(0);
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

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_MODIFIED);
        assertStatus(LOAD_ERROR);
        assertTotalErrors(1);
        assertErrorMessage(0, "Blank filepath set in data for node QueryLimitConfig/path");
        assertErrorDoesNotHaveStackTrace(0);
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

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_MODIFIED);
        assertStatus(LOAD_ERROR);
        assertTotalErrors(1);
        assertErrorMessage(0, "Blank filepath set in data for node QueryLimitConfig/path");
        assertErrorDoesNotHaveStackTrace(0);
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

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_MODIFIED);
        assertStatus(LOAD_ERROR);
        assertTotalErrors(1);
        assertErrorMessage(0, "Failed to read contents from file ftp://i/do/not/exist: Unsupported URI scheme: ftp");
        assertErrorStackTraceBeginsWith(0, "java.io.IOException: Unsupported URI scheme: ftp");
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

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_MODIFIED);
        assertStatus(LOAD_ERROR);
        assertTotalErrors(1);
        assertErrorMessage(0, "File not found: i/do/not/exist");
        assertErrorStackTraceBeginsWith(0, "java.nio.file.NoSuchFileException: i/do/not/exist");
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

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_MODIFIED);
        assertStatus(LOAD_ERROR);
        assertTotalErrors(1);
        assertErrorMessage(0, "File " + unsupportedFormatFile + " must be XML, JSON, or YAML");
        assertErrorDoesNotHaveStackTrace(0);
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

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_MODIFIED);
        assertStatus(LOAD_ERROR);
        assertTotalErrors(1);
        assertErrorMessage(0, "Failed to deserialize file to a datawave.webservice.query.limit.QueryLimitConfiguration");
        assertErrorStackTraceBeginsWith(0, "com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException: Unrecognized field \"property1\"");
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

        // Create the publisher and start listening for trigger events.
        createPublisher();

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
        assertCause(TRIGGER_NODE_MODIFIED);
        assertStatus(LOAD_ERROR);
        assertTotalErrors(1);
        assertErrorMessage(0,
                        "Reloaded datawave.webservice.query.limit.QueryLimitConfiguration failed validation: Default user query limit must be greater than 0");
        assertErrorStackTraceBeginsWith(0, "java.lang.IllegalArgumentException: Default user query limit must be greater than 0");
        assertTimeNodeHasRecentTime();
    }

    /**
     * Verify that if exceptions are thrown by subscribers after supplying them with a new configuration, the errors are captured and recorded.
     */
    @Test
    void testExceptionsThrownBySubscribers() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validJsonFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the publisher and start listening for trigger events.
        createPublisher();

        // Add listeners to the publisher that will throw a variety of exceptions.
        publisher.subscribeToUpdates(configuration -> {
            throw new NullPointerException("Something bad happened!");
        });
        publisher.subscribeToUpdates(configuration -> {
            throw new IllegalArgumentException("I don't like this configuration.");
        });
        publisher.subscribeToUpdates(configuration -> {
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
        assertCause(TRIGGER_NODE_MODIFIED);
        assertStatus(SUBSCRIBER_ERROR);
        assertTotalErrors(3);
        assertErrorMessage(0, "Exception thrown by listener: Something bad happened!");
        assertErrorStackTraceBeginsWith(0, "java.lang.NullPointerException: Something bad happened!");
        assertErrorMessage(1, "Exception thrown by listener: I don't like this configuration.");
        assertErrorStackTraceBeginsWith(1, "java.lang.IllegalArgumentException: I don't like this configuration.");
        assertErrorMessage(2, "Exception thrown by listener: Why do I even exist?");
        assertErrorStackTraceBeginsWith(2, "java.lang.UnsupportedOperationException: Why do I even exist?");
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

    private void assertCause(ZkPojoPublisherImpl.PublishCause trigger) throws Exception {
        assertData(causeNode, trigger.toString());
    }

    private void assertStatus(ZkPojoPublisherImpl.PublishStatus status) throws Exception {
        assertData(statusNode, status.toString());
    }

    private void assertErrorsNodeDoesNotExist() throws Exception {
        Stat stat = client.checkExists().forPath(errorsNode);
        assertNull(stat, "Expected node " + errorsNode + " to not exist");
    }

    private void assertTotalErrors(int expected) throws Exception {
        Stat stat = client.checkExists().forPath(errorsNode);
        assertEquals(expected, stat.getNumChildren(), "Expected node " + errorsNode + " to have " + expected + " children");
    }

    private void assertErrorMessage(int errorIndex, String message) throws Exception {
        assertData(errorsNode + "/error_" + errorIndex + "/message", message);
    }

    private void assertErrorStackTraceBeginsWith(int errorIndex, String prefix) throws Exception {
        String path = errorsNode + "/error_" + errorIndex + "/stacktrace";
        Stat stat = client.checkExists().forPath(path);
        assertNotNull(stat, "Expected node " + path + " to exist");
        String data = new String(client.getData().forPath(path));
        assertTrue(data.startsWith(prefix));
    }

    private void assertErrorDoesNotHaveStackTrace(int errorIndex) throws Exception {
        String path = errorsNode + "/error_" + errorIndex + "/stacktrace";
        Stat stat = client.checkExists().forPath(path);
        assertNull(stat, "Expected node " + path + " to not exist");
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

    private void createPublisher() throws Exception {
        ZkClientBuilder clientBuilder = new ZkClientBuilder().withNamespace(NAMESPACE).withConnectString(server.getConnectString());
        
        publisher = new ZkPojoPublisherImpl(clientBuilder, null, QueryLimitConfiguration.class);
        try {
            ThreadUtil.blockUntil(TimeUnit.SECONDS.toMillis(5), 100, () -> publisher.areCachesInitialized());
        } catch (Exception e) {
            throw new RuntimeException("Publisher caches failed to initialize before timeout", e);
        }
        publisher.subscribeToUpdates((pojo) -> configs.add((QueryLimitConfiguration) pojo));
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
