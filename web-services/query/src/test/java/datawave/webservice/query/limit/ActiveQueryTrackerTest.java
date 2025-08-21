package datawave.webservice.query.limit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ActiveQueryTrackerTest {
    
    private static final Pattern heartbeatNodeNamePattern = Pattern.compile(
                    "_c_[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}-heartbeat_\\d{10}");
    
    TestingServer server;
    ActiveQueryTracker tracker;
    
    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
        tracker = new ActiveQueryTracker(server.getConnectString(), 120000);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if(server != null) {
            server.close();
        }
    }
    
    @Nested @DisplayName("Given a single query")
    class SingleQuery {
        
        String queryId = UUID.randomUUID().toString();
        String user = "cn=test a. user, ou=example developers, o=example corp, c=us";
        String system = "server-01";
        String queryLogic = "ShardQueryLogic";
        
        @Nested @DisplayName("After tracking")
        class AfterTracking {
            
            @BeforeEach
            void setUp() {
                tracker.trackQuery(queryId, user, system, queryLogic);
            }
            
            @Test @DisplayName("it is tracked in Zookeeper")
            void createsZNodes() throws Exception {
                CuratorFramework client = getClient();
                client.start();
                
                // Verify that the nodes were created as expected in Zookeeper.
                assertThat(client.checkExists().forPath("/users/" + user + "/" + queryId)).isNotNull();
                assertThat(client.checkExists().forPath("/systems/server-01/" + queryId)).isNotNull();
                assertThat(client.checkExists().forPath("/queryLogics/ShardQueryLogic/" + queryId)).isNotNull();
                assertThat(client.checkExists().forPath("/queries/" + queryId)).isNotNull();
                assertThat(new String(client.getData().forPath("/queries/" + queryId + "/user"))).isEqualTo(user);
                assertThat(new String(client.getData().forPath("/queries/" + queryId + "/system"))).isEqualTo(system);
                assertThat(new String(client.getData().forPath("/queries/" + queryId + "/queryLogic"))).isEqualTo(queryLogic);
                Stat stat = client.checkExists().forPath("/queries/" + queryId + "/heartbeats");
                assertThat(stat).isNotNull();
                assertThat(stat.getNumChildren()).isEqualTo(0);
            }
            
            @Test @DisplayName("A failure will not occur if tracked again")
            void allowsTrackingAgain() {
                assertThatNoException().isThrownBy(() -> tracker.trackQuery(queryId, user, system, queryLogic));
            }
            
            @Nested @DisplayName("After untracking")
            class AfterUntracking {
                
                @BeforeEach
                void setUp() {
                    tracker.stopTrackingQuery(queryId);
                }
                
                @Test @DisplayName("It is not tracked in Zookeeper")
                void deletesZNodes() throws Exception {
                    CuratorFramework client = getClient();
                    client.start();
                    
                    // Verify that the nodes were deleted as expected in Zookeeper.
                    assertThat(client.checkExists().forPath("/users/" + user + "/" + queryId)).isNull();
                    assertThat(client.checkExists().forPath("/systems/server-01/" + queryId)).isNull();
                    assertThat(client.checkExists().forPath("/queryLogics/ShardQueryLogic/" + queryId)).isNull();
                    assertThat(client.checkExists().forPath("/queries/" + queryId)).isNull();
                }
                
                @Test @DisplayName("A failure will not occur if untracked again")
                void allowsUntrackingAgain() {
                    assertThatNoException().isThrownBy(() -> tracker.stopTrackingQuery(queryId));
                }
                
                @Test @DisplayName("A heartbeat cannot be created")
                void cannotObtainHeartbeat() {
                    assertThatThrownBy(() -> tracker.createHeartbeat(queryId)).isInstanceOf(ActiveQueryException.class)
                                    .hasMessage("Query " + queryId + " is not being tracked");
                }
            }
            
            @Nested @DisplayName("When a single heartbeat is created")
            class WhenSingleHeartBeatCreated {
                
                QueryHeartbeat heartbeat;
                
                @BeforeEach
                void setUp() {
                    heartbeat = tracker.createHeartbeat(queryId);
                }
                
                @Test @DisplayName("The path is not blank")
                void hasNonBlankPath() {
                    assertThat(heartbeat.getPath().isBlank()).isFalse();
                }
                
                @Test @DisplayName("It has the correct query ID")
                void hasMatchingQueryId() {
                    assertThat(heartbeat.getQueryId()).isEqualTo(queryId);
                }
                
                @Test @DisplayName("It has the correct parent")
                void wasCreatedInCorrectPath() {
                    String path = heartbeat.getPath();
                    String parentPath = path.substring(0, path.lastIndexOf('/'));
                    assertThat(parentPath).isEqualTo("/queries/" + queryId + "/heartbeats");
                }
                
                @Test @DisplayName("It has the expected name format")
                void wasCreatedWithProtection() {
                    String path = heartbeat.getPath();
                    String nodeName = path.substring(path.lastIndexOf('/') + 1);
                    assertThat(heartbeatNodeNamePattern.matcher(nodeName).matches()).isTrue();
                }
                
                @Test @DisplayName("It has sequence 0")
                void wasCreatedWithSequenceOfZeroes() {
                    String path = heartbeat.getPath();
                    String sequence = path.substring(path.lastIndexOf('_') + 1);
                    assertThat(sequence).isEqualTo("0000000000");
                }
                
                @Test @DisplayName("The query cannot be untracked")
                void cannotBeUntracked() {
                    assertThatThrownBy(() -> tracker.stopTrackingQuery(queryId)).isInstanceOf(ActiveQueryException.class)
                                    .hasMessage("Cannot stop tracking query " + queryId + ", 1 heartbeat(s) exist");
                }
                
                @Nested @DisplayName("When fetching snapshot")
                class WhenFetchingSnapshot {
                    
                    @Test @DisplayName("It is captured for a matching userDn")
                    void isCapturedForMatchingUser() throws Exception {
                        ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, "otherSystem", "otherQueryLogic");
                        
                        assertThat(snapshot.getUserDn()).isEqualTo(user);
                        assertThat(snapshot.getSystemName()).isEqualTo("otherSystem");
                        assertThat(snapshot.getQueryLogic()).isEqualTo("otherQueryLogic");
                        assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(0);
                        assertThat(snapshot.getTotalUserQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalSystemQueries()).isEqualTo(0);
                        assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalUserQueriesPerSystem()).isEqualTo(Map.of("server-01", 1));
                    }
                    
                    @Test @DisplayName("It is captured for a matching system")
                    void isCapturedForMatchingSystem() throws Exception {
                        ActiveQuerySnapshot snapshot = tracker.getSnapshot("otherUser", system, "otherQueryLogic");
                        
                        assertThat(snapshot.getUserDn()).isEqualTo("otherUser");
                        assertThat(snapshot.getSystemName()).isEqualTo(system);
                        assertThat(snapshot.getQueryLogic()).isEqualTo("otherQueryLogic");
                        assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(0);
                        assertThat(snapshot.getTotalUserQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalSystemQueries()).isEqualTo(1);
                        assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalUserQueriesPerSystem()).isEqualTo(Map.of());
                    }
                    
                    @Test @DisplayName("It is captured for a matching query logic")
                    void isCapturedForMatchingQueryLogic() throws Exception {
                        ActiveQuerySnapshot snapshot = tracker.getSnapshot("otherUser", "otherSystem", queryLogic);
                        
                        assertThat(snapshot.getUserDn()).isEqualTo("otherUser");
                        assertThat(snapshot.getSystemName()).isEqualTo("otherSystem");
                        assertThat(snapshot.getQueryLogic()).isEqualTo(queryLogic);
                        assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(0);
                        assertThat(snapshot.getTotalUserQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalSystemQueries()).isEqualTo(0);
                        assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(1);
                        assertThat(snapshot.getTotalUserQueriesPerSystem()).isEqualTo(Map.of());
                    }
                    
                    @Test @DisplayName("It is captured for a matching userDn and system")
                    void isCapturedForMatchingUserAndSystem() throws Exception {
                        ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, system, "otherQueryLogic");
                        
                        assertThat(snapshot.getUserDn()).isEqualTo(user);
                        assertThat(snapshot.getSystemName()).isEqualTo(system);
                        assertThat(snapshot.getQueryLogic()).isEqualTo("otherQueryLogic");
                        assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(1);
                        assertThat(snapshot.getTotalUserQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalSystemQueries()).isEqualTo(1);
                        assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalUserQueriesPerSystem()).isEqualTo(Map.of("server-01", 1));
                    }
                    
                    @Test @DisplayName("It is captured for a matching userDn and queryLogic")
                    void isCapturedForMatchingUserAndQueryLogic() throws Exception {
                        ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, "otherSystem", queryLogic);
                        
                        assertThat(snapshot.getUserDn()).isEqualTo(user);
                        assertThat(snapshot.getSystemName()).isEqualTo("otherSystem");
                        assertThat(snapshot.getQueryLogic()).isEqualTo(queryLogic);
                        assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(0);
                        assertThat(snapshot.getTotalUserQueriesForQueryLogic()).isEqualTo(1);
                        assertThat(snapshot.getTotalSystemQueries()).isEqualTo(0);
                        assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(1);
                        assertThat(snapshot.getTotalUserQueriesPerSystem()).isEqualTo(Map.of("server-01", 1));
                    }
                    
                    @Test @DisplayName("It is captured for a matching system and queryLogic")
                    void isCapturedForMatchingSystemAndQueryLogic() throws Exception {
                        ActiveQuerySnapshot snapshot = tracker.getSnapshot("otherUser", system, queryLogic);
                        
                        assertThat(snapshot.getUserDn()).isEqualTo("otherUser");
                        assertThat(snapshot.getSystemName()).isEqualTo(system);
                        assertThat(snapshot.getQueryLogic()).isEqualTo(queryLogic);
                        assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(0);
                        assertThat(snapshot.getTotalUserQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalSystemQueries()).isEqualTo(1);
                        assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(1);
                        assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(1);
                        assertThat(snapshot.getTotalUserQueriesPerSystem()).isEqualTo(Map.of());
                    }
                    
                    @Test @DisplayName("It is captured for a matching userDn, system, and queryLogic")
                    void isCapturedForMatchingUserAndSystemAndQueryLogic() throws Exception {
                        ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, system, queryLogic);
                        
                        assertThat(snapshot.getUserDn()).isEqualTo(user);
                        assertThat(snapshot.getSystemName()).isEqualTo(system);
                        assertThat(snapshot.getQueryLogic()).isEqualTo(queryLogic);
                        assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(1);
                        assertThat(snapshot.getTotalUserQueriesForQueryLogic()).isEqualTo(1);
                        assertThat(snapshot.getTotalSystemQueries()).isEqualTo(1);
                        assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(1);
                        assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(1);
                        assertThat(snapshot.getTotalUserQueriesPerSystem()).isEqualTo(Map.of("server-01", 1));
                    }
                }
                
                @Test @DisplayName("Is is captured in a snapshot")
                void isCapturedInSnapshot() {
                }
                
                @Nested @DisplayName("After stopping the heartbeat")
                class AfterStopping {
                    
                    @BeforeEach
                    void setUp() throws IOException {
                        heartbeat.stop();
                    }
                    
                    @Test @DisplayName("It is deleted in Zookeeper")
                    void deletesZNode() throws Exception {
                        CuratorFramework client = getClient();
                        client.start();
                        
                        Stat stat = client.checkExists().forPath("/queries/" + queryId + "/heartbeats");
                        assertThat(stat.getNumChildren()).isEqualTo(0);
                    }
                    
                    @Test @DisplayName("The path is null")
                    void hasNullPath() {
                        assertThat(heartbeat.getPath()).isNull();
                    }
                    
                    @Test @DisplayName("A failure will not occur if stopped again")
                    void allowsMultipleStops() throws IOException {
                        heartbeat.stop();
                    }
                    
                    @Test @DisplayName("The next heartbeat is still sequential")
                    void doesNotAffectSequencingOfFutureHeartbeats() {
                        QueryHeartbeat heartbeat = tracker.createHeartbeat(queryId);
                        String path = heartbeat.getPath();
                        String sequence = path.substring(path.lastIndexOf('_') + 1);
                        assertThat(sequence).isEqualTo("0000000001");
                    }
                    
                    @Test @DisplayName("It is not captured in snapshot")
                    void isNotCapturedInSnapshot() throws Exception {
                        ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, system, queryLogic);
                        
                        assertThat(snapshot).isNotNull();
                        assertThat(snapshot.getUserDn()).isEqualTo(user);
                        assertThat(snapshot.getSystemName()).isEqualTo(system);
                        assertThat(snapshot.getQueryLogic()).isEqualTo(queryLogic);
                        assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(0);
                        assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalSystemQueries()).isEqualTo(0);
                        assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(0);
                        assertThat(snapshot.getTotalUserQueriesPerSystem()).isEmpty();
                    }
                    
                    @Test @DisplayName("The query can be untracked")
                    void canBeUntracked() throws Exception {
                        tracker.stopTrackingQuery(queryId);
                        
                        CuratorFramework client = getClient();
                        client.start();
                        
                        // Verify that the nodes were deleted as expected in Zookeeper.
                        assertThat(client.checkExists().forPath("/users/" + user + "/" + queryId)).isNull();
                        assertThat(client.checkExists().forPath("/systems/server-01/" + queryId)).isNull();
                        assertThat(client.checkExists().forPath("/queryLogics/ShardQueryLogic/" + queryId)).isNull();
                        assertThat(client.checkExists().forPath("/queries/" + queryId)).isNull();
                    }
                }
            }
            @Nested @DisplayName("When multiple heartbeat are created")
            class WhenMultipleHeartbeatsCreated {
                
                private final List<QueryHeartbeat> heartbeats = new ArrayList<>();
                
                @BeforeEach
                void setUp() {
                    CuratorFramework client = getClient();
                    client.start();
                    heartbeats.add(tracker.createHeartbeat(queryId));
                    heartbeats.add(tracker.createHeartbeat(queryId));
                    heartbeats.add(tracker.createHeartbeat(queryId));
                }
                
                @AfterEach
                void tearDown() {
                    heartbeats.clear();
                }
                
                @Test @DisplayName("It is captured in a snapshot when one heartbeat is not stopped")
                void isCapturedInSnapshotWhenOneHeartbeatRemains() throws Exception {
                    heartbeats.get(0).stop();
                    heartbeats.get(1).stop();
                    
                    ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, system, queryLogic);
                    
                    assertThat(snapshot.getUserDn()).isEqualTo(user);
                    assertThat(snapshot.getSystemName()).isEqualTo(system);
                    assertThat(snapshot.getQueryLogic()).isEqualTo(queryLogic);
                    assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(1);
                    assertThat(snapshot.getTotalUserQueriesForQueryLogic()).isEqualTo(1);
                    assertThat(snapshot.getTotalSystemQueries()).isEqualTo(1);
                    assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(1);
                    assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(1);
                    assertThat(snapshot.getTotalUserQueriesPerSystem()).isEqualTo(Map.of("server-01", 1));
                }
                
                @Test @DisplayName("It is not captured in a snapshot when all heartbeats are stopped")
                void isNotCapturedInSnapshotWhenNoHeartbeatsRemain() throws Exception {
                    heartbeats.get(0).stop();
                    heartbeats.get(1).stop();
                    heartbeats.get(2).stop();
                    
                    ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, system, queryLogic);
                    
                    assertThat(snapshot.getUserDn()).isEqualTo(user);
                    assertThat(snapshot.getSystemName()).isEqualTo(system);
                    assertThat(snapshot.getQueryLogic()).isEqualTo(queryLogic);
                    assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(0);
                    assertThat(snapshot.getTotalUserQueriesForQueryLogic()).isEqualTo(0);
                    assertThat(snapshot.getTotalSystemQueries()).isEqualTo(0);
                    assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(0);
                    assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(0);
                    assertThat(snapshot.getTotalUserQueriesPerSystem()).isEqualTo(Map.of());
                }
            }
        }
        
        @Nested @DisplayName("When a query has never been tracked")
        public class WhenNeverTracked {
            
            @Test @DisplayName("A heartbeat cannot be created")
            void cannotCreateHeartbeat() {
                assertThatThrownBy(() -> tracker.createHeartbeat(queryId)).isInstanceOf(ActiveQueryException.class).hasMessage("Query " + queryId + " is not being tracked");
            }
            
            @Test @DisplayName("It is not captured in snapshot")
            void isNotCapturedInSnapshot() throws Exception {
                ActiveQuerySnapshot snapshot = tracker.getSnapshot(user, system, queryLogic);
                
                assertThat(snapshot.getUserDn()).isEqualTo(user);
                assertThat(snapshot.getSystemName()).isEqualTo(system);
                assertThat(snapshot.getQueryLogic()).isEqualTo(queryLogic);
                assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(0);
                assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(0);
                assertThat(snapshot.getTotalSystemQueries()).isEqualTo(0);
                assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(0);
                assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(0);
                assertThat(snapshot.getTotalUserQueriesPerSystem()).isEqualTo(Map.of());
            }
            
            @Test @DisplayName("A failure will not occur if explicitly untracked")
            void allowsUntrackingAgain() {
                assertThatNoException().isThrownBy(() -> tracker.stopTrackingQuery(queryId));
            }
        }
    }
    
    @Nested @DisplayName("Given multiple queries")
    class MultipleQueries {
        
        private final Map<String, QueryHeartbeat> heartbeats = new HashMap<>();
        
        @BeforeEach
        void setUp() {
            // The following data assumes that we will be fetching a snapshot for user userA, on system SYSTEM-01, for query logic TLDQueryLogic.
            
            // Counts towards totals for user, system, and query logic.
            createActiveQuery("cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");
            createActiveQuery("cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");
            createActiveQuery("cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");
            createActiveQuery("cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");
            
            // Counts towards totals for system and query logic.
            createActiveQuery("cn=userB, c=us", "SYSTEM-01", "TLDQueryLogic");
            createActiveQuery("cn=userB, c=us", "SYSTEM-01", "TLDQueryLogic");
            createActiveQuery("cn=userB, c=us", "SYSTEM-01", "TLDQueryLogic");
            
            // Counts towards totals for user and system.
            createActiveQuery("cn=userA, c=us", "SYSTEM-01", "OtherQueryLogic");
            createActiveQuery("cn=userA, c=us", "SYSTEM-01", "OtherQueryLogic");
            
            // Counts towards totals for system.
            createActiveQuery("cn=userB, c=us", "SYSTEM-01", "OtherQueryLogic");
            createActiveQuery("cn=userB, c=us", "SYSTEM-01", "OtherQueryLogic");
            
            // Counts towards totals for user and query logic.
            createActiveQuery("cn=userA, c=us", "SYSTEM-02", "TLDQueryLogic");
            createActiveQuery("cn=userA, c=us", "SYSTEM-02", "TLDQueryLogic");
            createActiveQuery("cn=userA, c=us", "SYSTEM-02", "TLDQueryLogic");
            createActiveQuery("cn=userA, c=us", "SYSTEM-02", "TLDQueryLogic");
            
            // Counts towards totals for query logic.
            createActiveQuery("cn=userB, c=us", "SYSTEM-02", "TLDQueryLogic");
            createActiveQuery("cn=userB, c=us", "SYSTEM-02", "TLDQueryLogic");
            createActiveQuery("cn=userB, c=us", "SYSTEM-02", "TLDQueryLogic");
            
            // Counts towards totals for user.
            createActiveQuery("cn=userA, c=us", "SYSTEM-02", "OtherQueryLogic");
            createActiveQuery("cn=userA, c=us", "SYSTEM-02", "OtherQueryLogic");
            
            // These entries should not count towards any totals.
            createActiveQuery("cn=userB, c=us", "SYSTEM-02", "OtherQueryLogic");
            createActiveQuery("cn=userB, c=us", "SYSTEM-02", "OtherQueryLogic");
            createActiveQuery("cn=userC, c=us", "SYSTEM-03", "OtherQueryLogic");
            createActiveQuery("cn=userC, c=us", "SYSTEM-03", "OtherQueryLogic");
            createActiveQuery("cn=userC, c=us", "SYSTEM-03", "OtherQueryLogic");
        }
        
        private void createActiveQuery(String userDn, String system, String queryLogic) {
            String queryId = UUID.randomUUID().toString();
            tracker.trackQuery(queryId, userDn, system, queryLogic);
            QueryHeartbeat heartbeat = tracker.createHeartbeat(queryId);
            heartbeats.put(queryId, heartbeat);
        }
        
        @Test @DisplayName("All relevant queries are captured in snapshot")
        void isCapturedInSnapshot() throws Exception {
            ActiveQuerySnapshot snapshot = tracker.getSnapshot("cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");
            
            assertThat(snapshot.getUserDn()).isEqualTo("cn=userA, c=us");
            assertThat(snapshot.getSystemName()).isEqualTo("SYSTEM-01");
            assertThat(snapshot.getQueryLogic()).isEqualTo("TLDQueryLogic");
            assertThat(snapshot.getTotalUserQueriesPerSystem()).isEqualTo(Map.of("SYSTEM-01", 6, "SYSTEM-02", 6));
            assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(6);
            assertThat(snapshot.getTotalUserQueriesForQueryLogic()).isEqualTo(8);
            assertThat(snapshot.getTotalSystemQueries()).isEqualTo(11);
            assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(7);
            assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(14);
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
