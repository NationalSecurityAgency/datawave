package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
        cache = new QueryHeartbeatCache();
    }

    @AfterEach
    void tearDown() throws IOException {
        this.cache = null;
        if (this.server != null) {
            this.server.close();
        }
    }

    /**
     * Verify that adding and retrieving a heartbeat by query ID works.
     */
    @Test
    void testPut() throws InterruptedException {
        QueryHeartbeat heartbeat = createHeartbeat("queryId");
        cache.put("queryId", heartbeat);
        assertThat(cache.get("queryId") == heartbeat).isTrue();
    }

    /**
     * Verifying that stopping a heartbeat after it's been added to the cache will evict itself from the cache.
     */
    @Test
    void testStoppingHeartbeatEvictsSelfFromCache() throws InterruptedException, IOException {
        QueryHeartbeat heartbeat = createHeartbeat("queryId");
        cache.put("queryId", heartbeat);

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
        cache.put("queryId", heartbeat);

        cache.stopAndRemoveHeartbeat("queryId");

        assertThat(cache.get("queryId")).isNull();
        assertNodesStopped(heartbeat);
    }

    /**
     * Verify that when stopping a heartbeat for a mapping that does not exist, an exception is not thrown.
     */
    @Test
    void testStopAndRemoveHeartbeatWithoutMatch() {
        assertThatNoException().isThrownBy(() -> cache.stopAndRemoveHeartbeat("queryId"));
    }

    /**
     * Verify that {@link QueryHeartbeatCache#clear()} clears the cache.
     */
    @Test
    void testClear() throws InterruptedException {
        cache.put("queryId1", createHeartbeat("queryId1"));
        cache.put("queryId2", createHeartbeat("queryId2"));
        cache.put("queryId3", createHeartbeat("queryId3"));

        cache.clear();

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
