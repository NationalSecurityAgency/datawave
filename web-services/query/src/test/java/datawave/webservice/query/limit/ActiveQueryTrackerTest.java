package datawave.webservice.query.limit;

import static datawave.webservice.query.limit.QueryLimiterUtils.ZOOKEEPER_NAMESPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActiveQueryTrackerTest {

    TestingServer server;

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    /**
     * Verify the correct node is made when tracking a new query.
     */
    @Test
    void testTrackingQuery() throws Exception {
        String queryId = UUID.randomUUID().toString();
        String user = "cn=test a. user, ou=example developers, o=example corp, c=us";
        String system = "SERVER-01";
        String queryLogic = "ShardQueryLogic";

        try (CuratorFramework client = getClient()) {
            ActiveQueryTracker tracker = new ActiveQueryTracker(client);
            tracker.trackQuery(queryId, user, system, queryLogic);

            // Verify that the query logic was added under the /queryLogics path.
            assertThat(client.checkExists().forPath("/queryLogics/ShardQueryLogic")).isNotNull();

            // Verify that the node was created as expected in Zookeeper.
            Stat stat = new Stat();
            byte[] data = client.getData().storingStatIn(stat).forPath("/queries/" + queryId);
            assertThat(stat).isNotNull();

            // Verify the node data is correct.
            DataInput dataInput = new DataInputStream(new ByteArrayInputStream(data));
            assertThat(WritableUtils.readString(dataInput)).isEqualTo(user.toLowerCase());
            assertThat(WritableUtils.readString(dataInput)).isEqualTo(system);
            assertThat(WritableUtils.readString(dataInput)).isEqualTo(queryLogic);
        }
    }

    /**
     * Verify that an exception is thrown if we track an already tracked query.
     */
    @Test
    void testQueryCannotBeRetracked() throws Exception {
        String queryId = UUID.randomUUID().toString();
        String user = "cn=test a. user, ou=example developers, o=example corp, c=us";
        String system = "SERVER-01";
        String queryLogic = "ShardQueryLogic";

        try (CuratorFramework client = getClient()) {
            ActiveQueryTracker tracker = new ActiveQueryTracker(client);
            tracker.trackQuery(queryId, user, system, queryLogic);

            assertThatExceptionOfType(QueryAlreadyTrackedException.class).isThrownBy(() -> tracker.trackQuery(queryId, user, system, queryLogic));
        }
    }

    /**
     * Verify that when a heartbeat returned by {@link ActiveQueryTracker#trackQuery(String, String, String, String)} is stopped, that the ephemeral nodes are
     * deleted.
     */
    @Test
    void testStoppingHeartbeat() throws Exception {
        String queryId = UUID.randomUUID().toString();
        String user = "cn=test a. user, ou=example developers, o=example corp, c=us";
        String system = "SERVER-01";
        String queryLogic = "ShardQueryLogic";

        try (CuratorFramework client = getClient()) {
            ActiveQueryTracker tracker = new ActiveQueryTracker(client);
            QueryHeartbeat heartbeat = tracker.trackQuery(queryId, user, system, queryLogic);

            heartbeat.stop();

            // Verify the root queries path was not deleted.
            assertThat(client.checkExists().forPath("/queries")).isNotNull();

            // Verify the query logics node was not affected.
            assertThat(client.checkExists().forPath("/queryLogics/" + queryLogic)).isNotNull();

            // Verify the query ID node was deleted.
            assertThat(client.checkExists().forPath("/queries/" + queryId)).isNull();
        }
    }

    private CuratorFramework getClient() throws InterruptedException {
        // @formatter:off
        CuratorFramework client = CuratorFrameworkFactory.builder()
                        .connectString(server.getConnectString())
                        .namespace(ZOOKEEPER_NAMESPACE)
                        .sessionTimeoutMs(60000)
                        .connectionTimeoutMs(60000)
                        .retryPolicy(new RetryNTimes(10, 1000))
                        .build();
        // @formatter:on
        client.start();
        client.blockUntilConnected(5, TimeUnit.SECONDS);
        return client;
    }
}
