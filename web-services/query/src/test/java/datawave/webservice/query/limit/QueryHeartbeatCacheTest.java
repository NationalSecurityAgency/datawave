package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueryHeartbeatCacheTest {

    private TestingServer server;
    private QueryHeartbeatCache cache;

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
        cache = new QueryHeartbeatCache(10, TimeUnit.MINUTES);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (this.cache != null) {
            this.cache.close(true);
        }
        if (this.server != null) {
            this.server.close();
        }
    }
    
    @Test
    void testCleanupIntervalLessThanOne() {
        assertThatThrownBy(() -> new QueryHeartbeatCache(0, TimeUnit.MINUTES)).isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("cleanup interval must be greater than 0");
    }
    
    @Test
    void testNullCleanupUnit() {
        assertThatThrownBy(() -> new QueryHeartbeatCache(10, null)).isInstanceOf(NullPointerException.class)
                        .hasMessage("cleanup interval unit must not be null");
    }
    
    /**
     * Verify that adding and retrieving a heartbeat by query ID works.
     */
    @Test
    void testPut() throws InterruptedException {
        QueryHeartbeat heartbeat = createHeartbeat("queryId");
        cache.put(heartbeat);
        assertThat(cache.get("queryId") == heartbeat).isTrue();
    }

    /**
     * Verify that {@link QueryHeartbeatCache#getQueryIds()} returns the keyset of the underlying cache.
     */
    @Test
    void testGetQueryIds() throws InterruptedException {
        QueryHeartbeat heartbeat1 = createHeartbeat("queryId1");
        QueryHeartbeat heartbeat2 = createHeartbeat("queryId2");
        QueryHeartbeat heartbeat3 = createHeartbeat("queryId3");

        cache.put(heartbeat1);
        cache.put(heartbeat2);
        cache.put(heartbeat3);

        assertThat(cache.getQueryIds()).containsExactlyInAnyOrder("queryId1", "queryId2", "queryId3");
    }

    /**
     * Verify that {@link QueryHeartbeatCache#stopAndRemove(Collection)} removes the heartbeats, and does not throw an error if a mapping is not found.
     */
    @Test
    void testStopAndRemoveMultipleHeartbeats() throws InterruptedException {
        QueryHeartbeat heartbeat1 = createHeartbeat("queryId1");
        QueryHeartbeat heartbeat2 = createHeartbeat("queryId2");
        QueryHeartbeat heartbeat3 = createHeartbeat("queryId3");
        QueryHeartbeat heartbeat4 = createHeartbeat("queryId4");
        QueryHeartbeat heartbeat5 = createHeartbeat("queryId5");

        cache.put(heartbeat1);
        cache.put(heartbeat2);
        cache.put(heartbeat3);
        cache.put(heartbeat4);
        cache.put(heartbeat5);

        Set<String> queryIds = Set.of("queryId1", "queryId2", "nonExistentQueryId");
        cache.stopAndRemove(queryIds);

        assertThat(cache.getQueryIds()).containsExactlyInAnyOrder("queryId3", "queryId4", "queryId5");
    }

    /**
     * Verifying that stopping a heartbeat after it's been added to the cache will evict itself from the cache.
     */
    @Test
    void testStoppingHeartbeatEvictsSelfFromCache() throws InterruptedException, IOException {
        QueryHeartbeat heartbeat = createHeartbeat("queryId");
        cache.put(heartbeat);

        assertThat(cache.get("queryId")).isNotNull();

        // Stopping the heartbeat outside the cache should result in the cache being notified and evicting the heartbeat.
        heartbeat.stop();

        assertThat(cache.get("queryId")).isNull();
    }

    /**
     * Verify that when stopping and removing a heartbeat, the heartbeat is stopped and no longer present in the cache.
     */
    @Test
    void testStopAndRemoveHeartbeatWithMatch() throws InterruptedException {
        QueryHeartbeat heartbeat = createHeartbeat("queryId");
        cache.put(heartbeat);

        cache.stopAndRemove("queryId");

        assertThat(cache.get("queryId")).isNull();
        assertNodesStopped(heartbeat);
    }

    /**
     * Verify that when stopping a heartbeat for a mapping that does not exist, an exception is not thrown.
     */
    @Test
    void testStopAndRemoveHeartbeatWithoutMatch() {
        assertThatNoException().isThrownBy(() -> cache.stopAndRemove("queryId"));
    }

    /**
     * Verify that the scheduled cleanup task will remove any stopped heartbeats present in the cache.
     */
    @Test
    void testTimedCleanupOfStoppedHeartbeats() throws IOException, InterruptedException {
        // Create a cache with a cleanup interval of 1.5 seconds.
        QueryHeartbeatCache cache = new QueryHeartbeatCache(1500, TimeUnit.MILLISECONDS);

        // Create heartbeats.
        QueryHeartbeat heartbeat1 = createHeartbeat("queryId1");
        QueryHeartbeat heartbeat2 = createHeartbeat("queryId2");
        QueryHeartbeat heartbeat3 = createHeartbeat("queryId3");
        QueryHeartbeat heartbeat4 = createHeartbeat("queryId4");

        // Add them to the cache.
        cache.put(heartbeat1);
        cache.put(heartbeat2);
        cache.put(heartbeat3);
        cache.put(heartbeat4);

        // Clear the listeners for the first two heartbeats and stop them. They will not evict themselves from the cache.
        heartbeat1.clearListeners();
        heartbeat1.stop();
        heartbeat2.clearListeners();
        heartbeat2.stop();

        // Sleep 2 seconds.
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        // Verify that we no longer see the stopped heartbeats in the cache, but do see the still-active heartbeats.
        assertThat(cache.get(heartbeat1.getQueryId())).isNull();
        assertThat(cache.get(heartbeat2.getQueryId())).isNull();
        assertThat(cache.get(heartbeat3.getQueryId())).isNotNull();
        assertThat(cache.get(heartbeat4.getQueryId())).isNotNull();
    }

    /**
     * Verify that {@link QueryHeartbeatCache#close(boolean)} ()} clears the cache.
     */
    @Test
    void testClose() throws InterruptedException {
        cache.put(createHeartbeat("queryId1"));
        cache.put(createHeartbeat("queryId2"));
        cache.put(createHeartbeat("queryId3"));

        cache.close(true);

        assertThat(cache.get("queryId1")).isNull();
        assertThat(cache.get("queryId2")).isNull();
        assertThat(cache.get("queryId3")).isNull();
    }

    private QueryHeartbeat createHeartbeat(String queryId) throws InterruptedException {
        CuratorFramework client = getClient();

        List<PersistentNode> nodes = new ArrayList<>();
        nodes.add(createNode(client, "/path1"));
        nodes.add(createNode(client, "/path2"));
        nodes.add(createNode(client, "/path3"));

        return new QueryHeartbeat(queryId, nodes);
    }

    private CuratorFramework getClient() {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(1));
        client.start();
        return client;
    }

    private PersistentNode createNode(CuratorFramework client, String path) throws InterruptedException {
        PersistentNode node = new PersistentNode(client, CreateMode.EPHEMERAL, false, path, new byte[0], false);
        node.start();
        node.waitForInitialCreate(1, TimeUnit.SECONDS);
        return node;
    }

    private void assertNodesStopped(QueryHeartbeat heartbeat) {
        // Verify that all the nodes are started.
        heartbeat.getNodes().forEach(node -> assertThat(node.getActualPath()).isNull());
    }
}
