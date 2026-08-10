package datawave.zookeeper;

import static java.nio.file.Files.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link ZkClientBuilder}.
 */
class ZkClientBuilderTest {

    /**
     * Verify {@link ZkClientBuilder#ZkClientBuilder(ZkClientBuilder)} returns a duplicate instance.
     */
    @Test
    void testCopyConstructor() {
        // @formatter:off
        ZkClientBuilder original = new ZkClientBuilder()
                        .setConnectString("localhost:2181")
                        .setNamespace("ns")
                        .setSessionTimeoutMs(1000)
                        .setConnectionTimeoutMs(2000)
                        .setRetryPolicyBuilder(new RetryNTimesBuilder().setMaxRetries(9));
        // @formatter:on

        ZkClientBuilder copy = new ZkClientBuilder(original);

        assertEquals(original, copy);
        assertNotSame(original, copy);
        // The retry policy builder should be a distinct, duplicated instance rather than a shared reference.
        assertNotSame(original.getRetryPolicyBuilder(), copy.getRetryPolicyBuilder());

        copy.setConnectString("otherhost:2181");

        assertEquals("localhost:2181", original.getConnectString());
        assertEquals("otherhost:2181", copy.getConnectString());
    }

    /**
     * Verify that {@link ZkClientBuilder#ZkClientBuilder(ZkClientBuilder)} will handle a null retry policy builder.
     */
    @Test
    void testCopyConstructorWithNullRetryPolicyBuilder() {
        ZkClientBuilder original = new ZkClientBuilder();
        original.setRetryPolicyBuilder(null);

        ZkClientBuilder copy = new ZkClientBuilder(original);

        assertNull(copy.getRetryPolicyBuilder());
    }

    /**
     * Verify {@link ZkClientBuilder#duplicate()} returns a duplicate instance.
     */
    @Test
    void testDuplicate() {
        ZkClientBuilder original = new ZkClientBuilder().setConnectString("localhost:2181").setNamespace("ns");
        ZkClientBuilder duplicate = original.duplicate();

        assertNotSame(original, duplicate);
        assertEquals(original, duplicate);
    }

    /**
     * Verify we can create a {@link CuratorFrameworkFactory} instance with a plain connect string.
     */
    @Test
    void testCreateFactoryBuilderWithPlainConnectString() throws QuorumPeerConfig.ConfigException {
        // @formatter:off
        ZkClientBuilder builder = new ZkClientBuilder()
                        .setConnectString("localhost:2181")
                        .setNamespace("ns")
                        .setSessionTimeoutMs(30_000)
                        .setConnectionTimeoutMs(30_000);
        // @formatter:on

        CuratorFrameworkFactory.Builder factoryBuilder = builder.createFactoryBuilder();

        assertNotNull(factoryBuilder);
    }

    /**
     * Verify that if the connect string an invalid Zookeeper config file, an exception will be thrown.
     */
    @Test
    void testCreateFactoryBuilderPropagatesConfigExceptionForInvalidZkConfigFile(@TempDir Path tempDir) throws Exception {
        Path cfgFile = tempDir.resolve("invalid.cfg");
        write(cfgFile, "tickTime=2000".getBytes());

        ZkClientBuilder builder = new ZkClientBuilder().setConnectString(cfgFile.toString());

        assertThrows(QuorumPeerConfig.ConfigException.class, builder::createFactoryBuilder);
    }

    /**
     * Verify {@link ZkClientBuilder#build()} returns a new, non-started {@link CuratorFramework}.
     */
    @Test
    void testBuildReturnsNonStartedClient() throws Exception {
        ZkClientBuilder builder = new ZkClientBuilder().setConnectString("localhost:2181");

        CuratorFramework client = builder.build();

        assertNotNull(client);
        assertEquals(CuratorFrameworkState.LATENT, client.getState());
    }

    /**
     * Verify that {@link ZkClientBuilder#buildAndStart(int, TimeUnit)} throws a {@link TimeoutException} when the created {@link CuratorFramework} cannot
     * connect to Zookeeper.
     */
    @Test
    @Timeout(5)
    void testBuildAndStartThrowsExceptionOnTimeout() {
        // @formatter:off
        ZkClientBuilder builder = new ZkClientBuilder()
                        .setConnectString("localhost:1")
                        .setRetryPolicyBuilder(new RetryNTimesBuilder()
                                        .setMaxRetries(0)
                                        .setSleepBetweenRetriesMs(0));
        // @formatter:on
        assertThrows(TimeoutException.class, () -> builder.buildAndStart(2, TimeUnit.SECONDS));
    }

    /**
     * Verify that {@link ZkClientBuilder#buildAndStart(int, TimeUnit)} returns a started {@link CuratorFramework} when the client can reach Zookeeper within
     * the timeout.
     */
    @Test
    @Timeout(5)
    void testBuildAndStartWithoutTimeout() throws Exception {
        try (TestingServer server = new TestingServer(true)) {
            // @formatter:off
            ZkClientBuilder builder = new ZkClientBuilder()
                            .setConnectString(server.getConnectString())
                            .setRetryPolicyBuilder(new RetryNTimesBuilder()
                                            .setMaxRetries(1)
                                            .setSleepBetweenRetriesMs(0));
            // @formatter:on
            try (CuratorFramework client = builder.buildAndStart(3, TimeUnit.SECONDS)) {
                assertNotNull(client);
                assertEquals(CuratorFrameworkState.STARTED, client.getState());
            }
        }
    }
}
