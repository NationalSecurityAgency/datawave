package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ActiveQueryTrackerTest {

    TestingServer server;
    ActiveQueryTracker tracker;

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
        tracker = new ActiveQueryTracker(server.getConnectString(), 120000);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    @Nested
    @DisplayName("Given a single query")
    class SingleQuery {

        String queryId = UUID.randomUUID().toString();
        String user = "cn=test a. user, ou=example developers, o=example corp, c=us";
        String system = "SERVER-01";
        String queryLogic = "ShardQueryLogic";
        QueryHeartbeat heartbeat;

        @Nested
        @DisplayName("After tracking")
        class AfterTracking {

            @BeforeEach
            void setUp() throws Exception {
                heartbeat = tracker.trackQuery(queryId, user, system, queryLogic);
                // tracker.trackQuery(queryId, user, system, queryLogic);
            }

            @Test
            @DisplayName("it is tracked in Zookeeper")
            void createsZNodes() throws Exception {
                CuratorFramework client = getClient();
                client.start();

                // Verify that the nodes were created as expected in Zookeeper.
                assertThat(client.checkExists().forPath("/users/" + user + "/" + queryId)).isNotNull();
                assertThat(client.checkExists().forPath("/systems/SERVER-01/" + queryId)).isNotNull();
                assertThat(client.checkExists().forPath("/queryLogics/ShardQueryLogic/" + queryId)).isNotNull();
                assertThat(client.checkExists().forPath("/distinctQueryLogics/" + queryLogic)).isNotNull();
                assertThat(client.checkExists().forPath("/queries/" + queryId)).isNotNull();
                byte[] data = client.getData().forPath("/queries/" + queryId);
                assertThat(new String(data)).isEqualTo(user + "\0" + system + "\0" + queryLogic);
            }

            @Test
            @DisplayName("A failure will occur if tracked again")
            void allowsTrackingAgain() {
                assertThatExceptionOfType(QueryAlreadyTrackedException.class).isThrownBy(() -> tracker.trackQuery(queryId, user, system, queryLogic));
            }

            @Test
            @DisplayName("The query logic is returned within the distinct query logics")
            void canFetchQueryLogics() {
                assertThat(tracker.getDistinctQueryLogics()).contains(queryLogic);
            }

            @Nested
            @DisplayName("When fetching snapshot")
            class WhenFetchingSnapshot {

                @Test
                @DisplayName("It is captured for a matching userDn")
                void isCapturedForMatchingUser() throws Exception {
                    Set<String> queryLogics = Set.of("otherQueryLogic");
                    ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, "otherSystem", queryLogics);

                    assertThat(snapshot.getUserDn()).isEqualTo(user);
                    assertThat(snapshot.getSystem()).isEqualTo("otherSystem");
                    assertThat(snapshot.getQueryLogics()).isEqualTo(queryLogics);
                    assertThat(snapshot.getTotalSystemQueries()).isEqualTo(0);
                    assertThat(snapshot.getUserQueriesPerSystem()).isEqualTo(Map.of("SERVER-01", 1));
                    // @formatter:off
                    assertThat(snapshot.getTotalUserQueriesPerSystemPerQueryLogic()).isEqualTo(Map.of(
                                    "SERVER-01", Map.of("ShardQueryLogic", 1)));
                    // @formatter:on
                    assertThat(snapshot.getTotalSystemQueriesPerQueryLogic()).isEmpty();
                }

                @Test
                @DisplayName("It is captured for a matching system")
                void isCapturedForMatchingSystem() throws Exception {
                    Set<String> queryLogics = Set.of("otherQueryLogic");
                    ActiveQuerySnapshot snapshot = tracker.getSnapshot("otherUser", system, queryLogics);

                    assertThat(snapshot.getUserDn()).isEqualTo("otherUser");
                    assertThat(snapshot.getSystem()).isEqualTo(system);
                    assertThat(snapshot.getQueryLogics()).isEqualTo(queryLogics);
                    assertThat(snapshot.getTotalSystemQueries()).isEqualTo(1);
                    assertThat(snapshot.getUserQueriesPerSystem()).isEqualTo(Map.of());
                    assertThat(snapshot.getTotalUserQueriesPerSystemPerQueryLogic()).isEmpty();
                    assertThat(snapshot.getTotalSystemQueriesPerQueryLogic()).isEqualTo(Map.of("ShardQueryLogic", 1));
                }

                @Test
                @DisplayName("It is captured for a matching query logic")
                void isCapturedForMatchingQueryLogic() throws Exception {
                    Set<String> queryLogics = Set.of(queryLogic);
                    ActiveQuerySnapshot snapshot = tracker.getSnapshot("otherUser", "otherSystem", queryLogics);

                    assertThat(snapshot.getUserDn()).isEqualTo("otherUser");
                    assertThat(snapshot.getSystem()).isEqualTo("otherSystem");
                    assertThat(snapshot.getQueryLogics()).isEqualTo(queryLogics);
                    assertThat(snapshot.getTotalSystemQueries()).isEqualTo(0);
                    assertThat(snapshot.getUserQueriesPerSystem()).isEqualTo(Map.of());
                    assertThat(snapshot.getTotalUserQueriesPerSystemPerQueryLogic()).isEmpty();
                    assertThat(snapshot.getTotalSystemQueriesPerQueryLogic()).isEmpty();
                }

                @Test
                @DisplayName("It is captured for a matching userDn and system")
                void isCapturedForMatchingUserAndSystem() throws Exception {
                    Set<String> queryLogics = Set.of("otherQueryLogic");
                    ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, system, queryLogics);

                    assertThat(snapshot.getUserDn()).isEqualTo(user);
                    assertThat(snapshot.getSystem()).isEqualTo(system);
                    assertThat(snapshot.getQueryLogics()).isEqualTo(queryLogics);
                    assertThat(snapshot.getTotalSystemQueries()).isEqualTo(1);
                    assertThat(snapshot.getUserQueriesPerSystem()).isEqualTo(Map.of("SERVER-01", 1));
                    // @formatter:off
                    assertThat(snapshot.getTotalUserQueriesPerSystemPerQueryLogic()).isEqualTo(Map.of(
                                    "SERVER-01", Map.of("ShardQueryLogic", 1)));
                    // @formatter:on
                    assertThat(snapshot.getTotalSystemQueriesPerQueryLogic()).isEqualTo(Map.of("ShardQueryLogic", 1));
                }

                @Test
                @DisplayName("It is captured for a matching userDn and queryLogic")
                void isCapturedForMatchingUserAndQueryLogic() throws Exception {
                    Set<String> queryLogics = Set.of(queryLogic);
                    ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, "otherSystem", queryLogics);

                    assertThat(snapshot.getUserDn()).isEqualTo(user);
                    assertThat(snapshot.getSystem()).isEqualTo("otherSystem");
                    assertThat(snapshot.getQueryLogics()).isEqualTo(queryLogics);
                    assertThat(snapshot.getTotalSystemQueries()).isEqualTo(0);
                    assertThat(snapshot.getUserQueriesPerSystem()).isEqualTo(Map.of("SERVER-01", 1));
                    // @formatter:off
                    assertThat(snapshot.getTotalUserQueriesPerSystemPerQueryLogic()).isEqualTo(Map.of(
                                    "SERVER-01", Map.of("ShardQueryLogic", 1)));
                    // @formatter:on
                    assertThat(snapshot.getTotalSystemQueriesPerQueryLogic()).isEmpty();
                }

                @Test
                @DisplayName("It is captured for a matching system and queryLogic")
                void isCapturedForMatchingSystemAndQueryLogic() throws Exception {
                    Set<String> queryLogics = Set.of(queryLogic);
                    ActiveQuerySnapshot snapshot = tracker.getSnapshot("otherUser", system, queryLogics);

                    assertThat(snapshot.getUserDn()).isEqualTo("otherUser");
                    assertThat(snapshot.getSystem()).isEqualTo(system);
                    assertThat(snapshot.getQueryLogics()).isEqualTo(queryLogics);
                    assertThat(snapshot.getTotalSystemQueries()).isEqualTo(1);
                    assertThat(snapshot.getUserQueriesPerSystem()).isEqualTo(Map.of());
                    assertThat(snapshot.getTotalUserQueriesPerSystemPerQueryLogic()).isEmpty();
                    assertThat(snapshot.getTotalSystemQueriesPerQueryLogic()).isEqualTo(Map.of("ShardQueryLogic", 1));
                }

                @Test
                @DisplayName("It is captured for a matching userDn, system, and queryLogic")
                void isCapturedForMatchingUserAndSystemAndQueryLogic() throws Exception {
                    Set<String> queryLogics = Set.of("otherQueryLogic");
                    // Pass in the userDN as uppercase and the system and query logic with whitespace to verify they are cleaned up.
                    ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, system, queryLogics);
                    assertThat(snapshot.getUserDn()).isEqualTo(user);
                    assertThat(snapshot.getSystem()).isEqualTo(system);
                    assertThat(snapshot.getQueryLogics()).isEqualTo(queryLogics);
                    assertThat(snapshot.getTotalSystemQueries()).isEqualTo(1);
                    assertThat(snapshot.getUserQueriesPerSystem()).isEqualTo(Map.of("SERVER-01", 1));
                    // @formatter:off
                    assertThat(snapshot.getTotalUserQueriesPerSystemPerQueryLogic()).isEqualTo(Map.of(
                                    "SERVER-01", Map.of("ShardQueryLogic", 1)));
                    // @formatter:on
                    assertThat(snapshot.getTotalSystemQueriesPerQueryLogic()).isEqualTo(Map.of("ShardQueryLogic", 1));
                }
            }

            @Nested
            @DisplayName("After stopping the heartbeat")
            class AfterStopping {

                @BeforeEach
                void setUp() throws IOException {
                    heartbeat.stop();
                }

                @Test
                @DisplayName("The query information is deleted in Zookeeper")
                void deletesZNode() throws Exception {
                    CuratorFramework client = getClient();
                    client.start();

                    // Verify the container paths were not deleted.
                    assertThat(client.checkExists().forPath("/queries")).isNotNull();
                    assertThat(client.checkExists().forPath("/users/" + user)).isNotNull();
                    assertThat(client.checkExists().forPath("/systems/" + system)).isNotNull();
                    assertThat(client.checkExists().forPath("/queryLogics/" + queryLogic)).isNotNull();

                    // Verify the nodes related to the specific query were deleted.
                    assertThat(client.checkExists().forPath("/queries/" + queryId)).isNull();
                    assertThat(client.checkExists().forPath("/users/" + user + "/" + queryId)).isNull();
                    assertThat(client.checkExists().forPath("/systems/" + system + "/" + queryId)).isNull();
                    assertThat(client.checkExists().forPath("/queryLogics/" + queryLogic + "/" + queryId)).isNull();

                }

                @Test
                @DisplayName("It is not captured in snapshot")
                void isNotCapturedInSnapshot() throws Exception {
                    Set<String> queryLogics = Set.of(queryLogic);
                    ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, system, queryLogics);

                    assertThat(snapshot).isNotNull();
                    assertThat(snapshot.getUserDn()).isEqualTo(user);
                    assertThat(snapshot.getSystem()).isEqualTo(system);
                    assertThat(snapshot.getQueryLogics()).isEqualTo(queryLogics);
                    assertThat(snapshot.getTotalSystemQueries()).isEqualTo(0);
                    assertThat(snapshot.getUserQueriesPerSystem()).isEmpty();
                }

                @Test
                @DisplayName("An error does not occur if it is closed multiple times")
                void willNotThrowExceptionIfStoppedAgain() {
                    assertThatNoException().isThrownBy(() -> heartbeat.stop());
                }
            }
        }

        @Nested
        @DisplayName("When a query has never been tracked")
        public class WhenNeverTracked {

            @Test
            @DisplayName("It is not captured in snapshot")
            void isNotCapturedInSnapshot() throws Exception {
                Set<String> queryLogics = Set.of(queryLogic);
                ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, system, queryLogics);

                assertThat(snapshot.getUserDn()).isEqualTo(user);
                assertThat(snapshot.getSystem()).isEqualTo(system);
                assertThat(snapshot.getQueryLogics()).isEqualTo(queryLogics);
                assertThat(snapshot.getTotalSystemQueries()).isEqualTo(0);
                assertThat(snapshot.getUserQueriesPerSystem()).isEqualTo(Map.of());
            }

            @Test
            @DisplayName("There are no distinct query logics")
            void noDistinctQueryLogics() {
                assertThat(tracker.getDistinctQueryLogics()).isEmpty();
            }
        }
    }

    @Nested
    @DisplayName("Given multiple queries")
    class MultipleQueries {

        private final Map<String,QueryHeartbeat> heartbeats = new HashMap<>();

        @BeforeEach
        void setUp() throws Exception {
            // The following data assumes that we will be fetching a snapshot for user usera, on system SYSTEM-01, for query logic TLDQueryLogic.
            createActiveQuery("cn=usera, c=us", "SYSTEM-01", "TLDQueryLogic");
            createActiveQuery("cn=usera, c=us", "SYSTEM-01", "EventQueryLogic");
            createActiveQuery("cn=usera, c=us", "SYSTEM-01", "TLDQueryLogic");
            createActiveQuery("cn=usera, c=us", "SYSTEM-01", "TLDQueryLogic");
            createActiveQuery("cn=userb, c=us", "SYSTEM-01", "EventQueryLogic");
            createActiveQuery("cn=userb, c=us", "SYSTEM-01", "TLDQueryLogic");
            createActiveQuery("cn=userb, c=us", "SYSTEM-01", "TLDQueryLogic");
            createActiveQuery("cn=usera, c=us", "SYSTEM-01", "EdgeQueryLogic");
            createActiveQuery("cn=usera, c=us", "SYSTEM-01", "EdgeQueryLogic");
            createActiveQuery("cn=userb, c=us", "SYSTEM-01", "EdgeQueryLogic");
            createActiveQuery("cn=userb, c=us", "SYSTEM-01", "EventQueryLogic");
            createActiveQuery("cn=usera, c=us", "SYSTEM-02", "TLDQueryLogic");
            createActiveQuery("cn=usera, c=us", "SYSTEM-02", "TLDQueryLogic");
            createActiveQuery("cn=usera, c=us", "SYSTEM-02", "TLDQueryLogic");
            createActiveQuery("cn=usera, c=us", "SYSTEM-02", "EventQueryLogic");
            createActiveQuery("cn=userb, c=us", "SYSTEM-02", "TLDQueryLogic");
            createActiveQuery("cn=userb, c=us", "SYSTEM-02", "TLDQueryLogic");
            createActiveQuery("cn=userb, c=us", "SYSTEM-02", "TLDQueryLogic");
            createActiveQuery("cn=usera, c=us", "SYSTEM-02", "EventQueryLogic");
            createActiveQuery("cn=usera, c=us", "SYSTEM-02", "EventQueryLogic");
            createActiveQuery("cn=userb, c=us", "SYSTEM-02", "EdgeQueryLogic");
            createActiveQuery("cn=userb, c=us", "SYSTEM-02", "EdgeQueryLogic");
            createActiveQuery("cn=userc, c=us", "SYSTEM-03", "EdgeQueryLogic");
            createActiveQuery("cn=userc, c=us", "SYSTEM-03", "EventQueryLogic");
            createActiveQuery("cn=userc, c=us", "SYSTEM-03", "EdgeQueryLogic");
        }

        private void createActiveQuery(String userDn, String system, String queryLogic) throws Exception {
            String queryId = UUID.randomUUID().toString();
            QueryHeartbeat heartbeat = tracker.trackQuery(queryId, userDn, system, queryLogic);
            heartbeats.put(queryId, heartbeat);
        }

        @Test
        @DisplayName("All relevant queries are captured in snapshot")
        void capturesEventForSingleQueryLogicInSnapshot() throws Exception {
            ActiveQuerySnapshot snapshot = tracker.getSnapshot("cn=usera, c=us", "SYSTEM-01", Set.of("TLDQueryLogic"));

            assertThat(snapshot.getUserDn()).isEqualTo("cn=usera, c=us");
            assertThat(snapshot.getSystem()).isEqualTo("SYSTEM-01");
            assertThat(snapshot.getQueryLogics()).containsExactly("TLDQueryLogic");
            assertThat(snapshot.getUserQueriesPerSystem()).isEqualTo(Map.of("SYSTEM-01", 6, "SYSTEM-02", 6));
            assertThat(snapshot.getTotalSystemQueries()).isEqualTo(11);
        }

        @Test
        @DisplayName("All relevant queries are captured in snapshot")
        void capturesEventForMultipleQueryLogicsInSnapshot() throws Exception {
            ActiveQuerySnapshot snapshot = tracker.getSnapshot("cn=usera, c=us", "SYSTEM-01", Set.of("TLDQueryLogic", "EventQueryLogic"));

            assertThat(snapshot.getUserDn()).isEqualTo("cn=usera, c=us");
            assertThat(snapshot.getSystem()).isEqualTo("SYSTEM-01");
            assertThat(snapshot.getQueryLogics()).containsExactlyInAnyOrder("TLDQueryLogic", "EventQueryLogic");
            assertThat(snapshot.getUserQueriesPerSystem()).isEqualTo(Map.of("SYSTEM-01", 6, "SYSTEM-02", 6));
            assertThat(snapshot.getTotalSystemQueries()).isEqualTo(11);
        }

        @Test
        @DisplayName("All distinct query logics were tracked")
        void retainedDistinctQueryLogics() {
            assertThat(tracker.getDistinctQueryLogics()).containsExactlyInAnyOrder("TLDQueryLogic", "EdgeQueryLogic", "EventQueryLogic");
        }

        @AfterEach
        void tearDown() {
            heartbeats.clear();
        }
    }

    private CuratorFramework getClient() {
        // @formatter:off
        return CuratorFrameworkFactory.builder()
                        .connectString(server.getConnectString())
                        .namespace("ActiveQueries")
                        .sessionTimeoutMs(60000)
                        .connectionTimeoutMs(60000)
                        .retryPolicy(new RetryNTimes(10, 1000))
                        .build();
        // @formatter:on
    }
}
