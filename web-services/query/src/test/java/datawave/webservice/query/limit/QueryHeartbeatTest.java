package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.awaitility.Awaitility;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueryHeartbeatTest {

    private TestingServer server;
    private CuratorFramework client;

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(10));
        client.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.close();
        }
        if (this.client != null) {
            this.client.close();
        }
    }

    /**
     * Verify that when {@link QueryHeartbeat#stop()} is called, that all underlying nodes are closed, and no exception occurs if a listener has not been set.
     */
    @Test
    void testStopWithNullListener() throws Exception {
        QueryHeartbeat heartbeat = createHeartbeat();

        // Verify that all the nodes are started.
        assertNodesStarted(heartbeat);

        heartbeat.stop();

        // Verify that all the nodes were closed.
        assertNodesStopped(heartbeat);
    }

    /**
     * Verify that when {@link QueryHeartbeat#stop()} is called, that all underlying nodes are closed, and if a listener has been set, that the listener is
     * notified that the heartbeat stopped.
     */
    @Test
    void testStopWithListener() throws Exception {
        QueryHeartbeat heartbeat = createHeartbeat();

        // Create a mock listener.
        QueryHeartbeatCache.HeartbeatStoppedListener listener = EasyMock.createMock(QueryHeartbeatCache.HeartbeatStoppedListener.class);
        listener.heartbeatStopped("queryId");
        EasyMock.expectLastCall();
        EasyMock.replay(listener);
        heartbeat.setListener(listener);

        // Verify that all the nodes are started.
        assertNodesStarted(heartbeat);

        heartbeat.stop();

        // Verify that all the nodes were closed.
        assertNodesStopped(heartbeat);

        EasyMock.verify(listener);
    }

    /**
     * Verify that when {@link QueryHeartbeat#stopWithoutNotifyingListener()} ()} is called, that all underlying nodes are closed, and no exception occurs if a
     * listener has not been set.
     */
    @Test
    void testStopWithoutNotifyingListenerWithNoListener() throws Exception {
        QueryHeartbeat heartbeat = createHeartbeat();

        // Verify that all the nodes are started.
        assertNodesStarted(heartbeat);

        heartbeat.stopWithoutNotifyingListener();

        // Verify that all the nodes were closed.
        assertNodesStopped(heartbeat);
    }

    /**
     * Verify that when {@link QueryHeartbeat#stop()} is called, that all underlying nodes are closed, and if a listener has been set, that the listener is not
     * notified that the heartbeat stopped.
     */
    @Test
    void testStopWithoutNotifyingListenerWithListener() throws Exception {
        QueryHeartbeat heartbeat = createHeartbeat();

        // Create a mock listener. Do not expect any calls to be made to the listener.
        QueryHeartbeatCache.HeartbeatStoppedListener listener = EasyMock.createMock(QueryHeartbeatCache.HeartbeatStoppedListener.class);
        EasyMock.replay(listener);
        heartbeat.setListener(listener);

        // Verify that all the nodes are started.
        assertNodesStarted(heartbeat);

        heartbeat.stopWithoutNotifyingListener();

        // Verify that all the nodes were closed.
        assertNodesStopped(heartbeat);

        EasyMock.verify(listener);
    }

    /**
     * Verify that {@link QueryHeartbeat#isStopped()} returns false for a non-stopped heartbeat, and true for a stopped heartbeat.
     */
    @Test
    void testIsStopped() throws InterruptedException, IOException {
        QueryHeartbeat heartbeat = createHeartbeat();

        Awaitility.await("Awaiting node creation").atMost(5, TimeUnit.SECONDS).until(() -> !heartbeat.isStopped());

        heartbeat.stop();

        assertThat(heartbeat.isStopped()).isTrue();
    }

    private QueryHeartbeat createHeartbeat() throws InterruptedException {
        PersistentNode node = new PersistentNode(client, CreateMode.EPHEMERAL, false, "/path", new byte[0], false);
        node.start();
        node.waitForInitialCreate(1, TimeUnit.SECONDS);
        return new QueryHeartbeat("queryId", node);
    }

    private void assertNodesStarted(QueryHeartbeat heartbeat) {
        assertThat(heartbeat.getNode().getActualPath()).isNotNull();
    }

    private void assertNodesStopped(QueryHeartbeat heartbeat) {
        assertThat(heartbeat.getNode().getActualPath()).isNull();
    }
}
