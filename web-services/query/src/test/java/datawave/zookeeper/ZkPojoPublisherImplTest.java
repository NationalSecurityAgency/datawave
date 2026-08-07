package datawave.zookeeper;

import static datawave.zookeeper.ZkPojoPublisherImpl.Cause.PATH_NODE_CREATED;
import static datawave.zookeeper.ZkPojoPublisherImpl.Cause.PATH_NODE_MODIFIED;
import static datawave.zookeeper.ZkPojoPublisherImpl.Cause.TRIGGER_NODE_CREATED;
import static datawave.zookeeper.ZkPojoPublisherImpl.Cause.TRIGGER_NODE_DELETED;
import static datawave.zookeeper.ZkPojoPublisherImpl.Cause.TRIGGER_NODE_MODIFIED;
import static datawave.zookeeper.ZkPojoPublisherImpl.Status.*;
import static datawave.zookeeper.ZkPojoPublisherImpl.Status.LOAD_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
    
    private static final String NAMESPACE = "pojoPublishers/QueryLimitConfig";
    
    private static final JsonMapper jsonMapper = JsonMapper.builder().addModule(new JavaTimeModule()).build();
    
    private static String validJsonFile;
    private static String validXmlFile;
    private static String validYamlFile;
    private static String nonConfigFile;
    private static String unsupportedFormatFile;
    
    private ZkPojoPublisherImpl<QueryLimitConfiguration> publisher;
    private final List<QueryLimitConfiguration> configs = new ArrayList<>();
    private TestingServer server;
    private CuratorFramework client;
    
    private static String latestAttemptNode;
    
    private Instant testStartTime;
    private ZkPojoPublisherImpl.Status expectedStatus;
    private ZkPojoPublisherImpl.Cause expectedCause;
    private final List<ErrorAssertion> errorAssertions = new ArrayList<>();
    
    @BeforeAll
    static void beforeAll() throws Exception {
        String serverIpAddress = InetAddress.getLocalHost().getHostAddress();
        latestAttemptNode = "/attempts/" + serverIpAddress + "/latest";

        ClassLoader classLoader = ZkPojoPublisherImplTest.class.getClassLoader();
        validJsonFile = getAbsolutePath(classLoader, "queryLimits/valid_config.json");
        validXmlFile = getAbsolutePath(classLoader, "queryLimits/valid_config.xml");
        validYamlFile = getAbsolutePath(classLoader, "queryLimits/valid_config.yaml");
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
        expectedStatus = null;
        expectedCause = null;
        errorAssertions.clear();
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
        assertThatThrownBy(() -> new ZkPojoPublisherImpl<>(null, null, null)).isInstanceOf(NullPointerException.class)
                        .hasMessage("zkClientBuilder must not be null");
        assertThatThrownBy(() -> new ZkPojoPublisherImpl<>(new ZkClientBuilder(), null, null))
                        .isInstanceOf(NullPointerException.class).hasMessage("pojoClass must not be null");
    }

    /**
     * Verify that the {@link ZkPojoPublisherImpl} can read a POJO from a JSON file on the local file system.
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
        waitForLatestAttemptNodeToBeCreated();

        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_MODIFIED);
        expectStatus(SUCCESS);
        assertLatestAttempt();
    }

    /**
     * Verify that the {@link ZkPojoPublisherImpl} can read a POJO from an XML file on the local file system.
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
        waitForLatestAttemptNodeToBeCreated();
        
        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_MODIFIED);
        expectStatus(SUCCESS);
        
        assertLatestAttempt();
    }

    /**
     * Verify that the {@link ZkPojoPublisherImpl} can read a POJO from a YAML file on the local file system.
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
        waitForLatestAttemptNodeToBeCreated();
        
        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_MODIFIED);
        expectStatus(SUCCESS);
        
        assertLatestAttempt();
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
        waitForLatestAttemptNodeToBeCreated();
        
        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_MODIFIED);
        expectStatus(SUCCESS);
        
        assertLatestAttempt();
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
        waitForLatestAttemptNodeToBeCreated();
        
        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_CREATED);
        expectStatus(SUCCESS);
        
        assertLatestAttempt();
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
        waitForLatestAttemptNodeToBeCreated();
        
        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_DELETED);
        expectStatus(SUCCESS);
        
        assertLatestAttempt();
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
        
        // Verify the latest attempt node is never created.
        assertNodeDoesNotExist(latestAttemptNode);
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
        
        // Verify the latest attempt node is never created.
        assertNodeDoesNotExist(latestAttemptNode);
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
        waitForLatestAttemptNodeToBeCreated();

        // Verify the latest attempt node was updated correctly.
        expectCause(PATH_NODE_CREATED);
        expectStatus(SUCCESS);
        
        assertLatestAttempt();
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
        waitForLatestAttemptNodeToBeCreated();
        
        // Verify the latest attempt node was updated correctly.
        expectCause(PATH_NODE_MODIFIED);
        expectStatus(SUCCESS);
        
        assertLatestAttempt();
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
        waitForLatestAttemptNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());

        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_MODIFIED);
        expectStatus(LOAD_ERROR);
        expectError("Node does not exist: /path");
        
        assertLatestAttempt();
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
        waitForLatestAttemptNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());
        
        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_MODIFIED);
        expectStatus(LOAD_ERROR);
        expectError("Blank filepath set in data for node /path");
        
        assertLatestAttempt();
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
        waitForLatestAttemptNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());
        
        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_MODIFIED);
        expectStatus(LOAD_ERROR);
        expectError("Blank filepath set in data for node /path");
        
        assertLatestAttempt();
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
        waitForLatestAttemptNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());
        
        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_MODIFIED);
        expectStatus(LOAD_ERROR);
        expectError("Failed to read contents from file ftp://i/do/not/exist: Unsupported URI scheme: ftp", "java.io.IOException: Unsupported URI scheme: ftp");
        
        assertLatestAttempt();
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
        waitForLatestAttemptNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());
        
        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_DELETED);
        expectStatus(LOAD_ERROR);
        expectError("File not found: i/do/not/exist", "java.nio.file.NoSuchFileException: i/do/not/exist");
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
        waitForLatestAttemptNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());
        
        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_DELETED);
        expectStatus(LOAD_ERROR);
        expectError("File " + unsupportedFormatFile + " must be XML, JSON, or YAML");
    }

    /**
     * Verify we do not load a POJO when the node {@code /path} points to a file that cannot be deserialized as the POJO type.
     */
    @Test
    void testNonPojoFile() throws Exception {
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
        waitForLatestAttemptNodeToBeCreated();

        // Verify that a configuration was not supplied to the listener.
        assertTrue(configs.isEmpty());

        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_DELETED);
        expectStatus(LOAD_ERROR);
        expectError("Failed to deserialize file to a datawave.webservice.query.limit.QueryLimitConfiguration",
                        "com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException: Unrecognized field \"property1\"");
    }

    /**
     * Verify that if exceptions are thrown by listeners after supplying them with a new POJO, the errors are captured and recorded.
     */
    @Test
    void testExceptionsThrownByListeners() throws Exception {
        // Set up the /path node beforehand.
        createOrUpdateNode("/path", validJsonFile);
        createOrUpdateNode("/trigger", "changeme");

        // Create the publisher and start listening for trigger events.
        createPublisher();

        // Add listeners to the publisher that will throw a variety of exceptions.
        publisher.addListener(configuration -> {
            throw new NullPointerException("Something bad happened!");
        });
        publisher.addListener(configuration -> {
            throw new IllegalArgumentException("I don't like this configuration.");
        });
        publisher.addListener(configuration -> {
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
        waitForLatestAttemptNodeToBeCreated();
        
        // Verify the latest attempt node was updated correctly.
        expectCause(TRIGGER_NODE_DELETED);
        expectStatus(LOAD_ERROR);
        expectError("Exception thrown by listener: Something bad happened!", "java.lang.NullPointerException: Something bad happened!");
        expectError("Exception thrown by listener: I don't like this configuration.", "java.lang.IllegalArgumentException: I don't like this configuration.");
        expectError("Exception thrown by listener: Why do I even exist?", "java.lang.UnsupportedOperationException: Why do I even exist?");
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
     * Wait with a timeout until we know the node {@code attempts/<serverIpAddress>/latest} exists.
     */
    private void waitForLatestAttemptNodeToBeCreated() {
        try {
            Awaitility.await().atMost(4, TimeUnit.SECONDS).until(() -> client.checkExists().forPath(latestAttemptNode) != null);
        } catch (Exception e) {
            fail("Timeout exceeded while waiting for node " + latestAttemptNode + " to be created: " + e.getMessage());
        }
    }
    
    private void expectStatus(ZkPojoPublisherImpl.Status status) {
        this.expectedStatus = status;
    }
    
    private void expectCause(ZkPojoPublisherImpl.Cause cause) {
        this.expectedCause = cause;
    }
    
    private void expectError(String message) {
        this.errorAssertions.add(new ErrorAssertion(message));
    }
    
    private void expectError(String message, String stacktraceStart) {
        this.errorAssertions.add(new ErrorAssertion(message, stacktraceStart));
    }
    
    private void assertLatestAttempt() throws Exception {
        ZkPojoPublisherImpl.PublishAttempt actual = getAttempt();
        assertEquals(expectedCause, actual.getCause());
        assertEquals(expectedStatus, actual.getStatus());
        assertTrue(actual.getTime().isAfter(testStartTime));
        
        List<ZkPojoPublisherImpl.Error> actualErrors = actual.getErrors();
        assertThat(actualErrors).hasSize(errorAssertions.size());
        for(int i = 0; i < errorAssertions.size(); i++) {
            errorAssertions.get(i).assertMatches(actualErrors.get(i));
        }
    }
    
    private ZkPojoPublisherImpl.PublishAttempt getAttempt() throws Exception {
        Stat stat = client.checkExists().forPath(latestAttemptNode);
        if(stat == null) {
            fail("Expected node " + latestAttemptNode + " to exist");
        }
        byte[] data = client.getData().forPath(latestAttemptNode);
        return jsonMapper.readValue(data, ZkPojoPublisherImpl.PublishAttempt.class);
    }
    
    private void assertNodeDoesNotExist(String path) throws Exception {
        assertNull(client.checkExists().forPath(path));
    }
    
    private void createPublisher() throws Exception {
        ZkClientBuilder clientBuilder = new ZkClientBuilder().withNamespace(NAMESPACE).withConnectString(server.getConnectString());
        publisher = new ZkPojoPublisherImpl<>(clientBuilder, null, QueryLimitConfiguration.class);
        publisher.setup();
        try {
            ThreadUtil.blockUntil(TimeUnit.SECONDS.toMillis(5), 100, () -> publisher.areCachesInitialized());
        } catch (Exception e) {
            throw new RuntimeException("Publisher caches failed to initialize before timeout", e);
        }
        publisher.addListener(configs::add);
    }

    private void createOrUpdateNode(String node, String dataStr) throws Exception {
        Stat stat = client.checkExists().forPath(node);
        byte[] data = dataStr == null ? new byte[0] : dataStr.getBytes(StandardCharsets.UTF_8);
        if (stat == null) {
            client.create().forPath(node, data);
        } else {
            client.setData().forPath(node, data);
        }
    }
    
    private static class ErrorAssertion {
        private final String message;
        private final String stackTrackStart;
    
        private ErrorAssertion(String message) {
            this(message, null);
        }
        
        private ErrorAssertion(String message, String stackTrackStart) {
            this.message = message;
            this.stackTrackStart = stackTrackStart;
        }
        
        private void assertMatches(ZkPojoPublisherImpl.Error error) {
            assertEquals(message, error.getMessage());
            if(stackTrackStart != null) {
                assertTrue(error.getStacktrace().startsWith(stackTrackStart));
            } else {
                assertNull(error.getStacktrace());
            }
        }
    }
}
