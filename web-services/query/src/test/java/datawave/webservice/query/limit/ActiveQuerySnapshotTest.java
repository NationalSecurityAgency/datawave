package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

class ActiveQuerySnapshotTest {

    /**
     * Verify that when a timestamp is not provided, that it is set to the current time.
     */
    @Test
    void testDefaultTimestamp() {
        long before = System.currentTimeMillis();
        ActiveQuerySnapshot snapshot = ActiveQuerySnapshot.builder("cn=testuser, c=us", "SYSTEM-01", Set.of("TLDQueryLogic")).build();
        long after = System.currentTimeMillis();

        assertThat(before).isLessThanOrEqualTo(snapshot.getTimestamp());
        assertThat(after).isGreaterThanOrEqualTo(snapshot.getTimestamp());
    }

    /**
     * Verify that when a timestamp is provided, it is kept.
     */
    @Test
    void testOverriddenTimestamp() {
        long timestamp = System.currentTimeMillis() - 5000L;

        ActiveQuerySnapshot snapshot = ActiveQuerySnapshot.builder("cn=testuser, c=us", "SYSTEM-01", Set.of("TLDQueryLogic")).withTimestamp(timestamp).build();

        assertThat(snapshot.getTimestamp()).isEqualTo(timestamp);
    }

    /**
     * Verify that an empty snapshot is structured correctly.
     */
    @Test
    void testSnapshotWithNoQueries() {
        Set<String> queryLogics = Set.of("TLDQueryLogic");

        ActiveQuerySnapshot snapshot = ActiveQuerySnapshot.builder("cn=testuser, c=us", "SYSTEM-01", queryLogics).build();

        assertThat(snapshot.getUserDn()).isEqualTo("cn=testuser, c=us");
        assertThat(snapshot.getSystem()).isEqualTo("SYSTEM-01");
        assertThat(snapshot.getQueryLogics()).isEqualTo(queryLogics);
        assertThat(snapshot.getUserQueriesPerSystem()).isEmpty();
        assertThat(snapshot.getTotalSystemQueries()).isEqualTo(0);
    }

    /**
     * Verify that totals are calculated correctly when targeting a single query logic.
     */
    @Test
    void testSnapshotWithSingleQueryLogic() {
        Set<String> queryLogics = Set.of("TLDQueryLogic");

        ActiveQuerySnapshot.Builder builder = ActiveQuerySnapshot.builder("cn=userA, c=us", "SYSTEM-01", queryLogics);
        // Counts towards totals for user, system, and query logic.
        builder.capture("query-1", "cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");
        builder.capture("query-2", "cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");
        builder.capture("query-3", "cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");
        builder.capture("query-4", "cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");

        // Counts towards totals for system and query logic.
        builder.capture("query-5", "cn=userB, c=us", "SYSTEM-01", "TLDQueryLogic");
        builder.capture("query-6", "cn=userB, c=us", "SYSTEM-01", "TLDQueryLogic");
        builder.capture("query-7", "cn=userB, c=us", "SYSTEM-01", "TLDQueryLogic");

        // Counts towards totals for user and system.
        builder.capture("query-8", "cn=userA, c=us", "SYSTEM-01", "OtherQueryLogic");
        builder.capture("query-9", "cn=userA, c=us", "SYSTEM-01", "OtherQueryLogic");

        // Counts towards totals for system.
        builder.capture("query-10", "cn=userB, c=us", "SYSTEM-01", "OtherQueryLogic");
        builder.capture("query-11", "cn=userB, c=us", "SYSTEM-01", "OtherQueryLogic");

        // Counts towards totals for user and query logic.
        builder.capture("query-12", "cn=userA, c=us", "SYSTEM-02", "TLDQueryLogic");
        builder.capture("query-13", "cn=userA, c=us", "SYSTEM-02", "TLDQueryLogic");
        builder.capture("query-14", "cn=userA, c=us", "SYSTEM-02", "TLDQueryLogic");
        builder.capture("query-15", "cn=userA, c=us", "SYSTEM-02", "TLDQueryLogic");

        // Counts towards totals for query logic.
        builder.capture("query-16", "cn=userB, c=us", "SYSTEM-02", "TLDQueryLogic");
        builder.capture("query-17", "cn=userB, c=us", "SYSTEM-02", "TLDQueryLogic");
        builder.capture("query-18", "cn=userB, c=us", "SYSTEM-02", "TLDQueryLogic");

        // Counts towards totals for user.
        builder.capture("query-19", "cn=userA, c=us", "SYSTEM-02", "OtherQueryLogic");
        builder.capture("query-20", "cn=userA, c=us", "SYSTEM-02", "OtherQueryLogic");

        ActiveQuerySnapshot snapshot = builder.build();

        assertThat(snapshot.getUserDn()).isEqualTo("cn=userA, c=us");
        assertThat(snapshot.getSystem()).isEqualTo("SYSTEM-01");
        assertThat(snapshot.getQueryLogics()).isEqualTo(queryLogics);
        assertThat(snapshot.getUserQueriesPerSystem()).isEqualTo(Map.of("SYSTEM-01", 6, "SYSTEM-02", 6));
        assertThat(snapshot.getTotalSystemQueries()).isEqualTo(11);
        // @formatter:off
        assertThat(snapshot.getTotalUserQueriesPerSystemPerQueryLogic()).isEqualTo(Map.of(
                        "SYSTEM-01", Map.of("OtherQueryLogic", 2, "TLDQueryLogic", 4),
                        "SYSTEM-02", Map.of("OtherQueryLogic", 2, "TLDQueryLogic", 4)));
        // @formatter:on
        assertThat(snapshot.getTotalSystemQueriesPerQueryLogic()).isEqualTo(Map.of("OtherQueryLogic", 4, "TLDQueryLogic", 7));
    }

    /**
     * Verify that totals are calculated correctly when targeting multiple query logics.
     */
    @Test
    void testSnapshotWithMultipleQueryLogics() {
        Set<String> queryLogics = Set.of("TLDQueryLogic", "EventQueryLogic");

        ActiveQuerySnapshot.Builder builder = ActiveQuerySnapshot.builder("cn=userA, c=us", "SYSTEM-01", queryLogics);
        // Counts towards totals for user, system, and query logic.
        builder.capture("query-1", "cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");
        builder.capture("query-2", "cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");
        builder.capture("query-3", "cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");
        builder.capture("query-4", "cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");
        builder.capture("query-5", "cn=userA, c=us", "SYSTEM-01", "EventQueryLogic");

        // Counts towards totals for system and query logic.
        builder.capture("query-6", "cn=userB, c=us", "SYSTEM-01", "TLDQueryLogic");
        builder.capture("query-7", "cn=userB, c=us", "SYSTEM-01", "TLDQueryLogic");
        builder.capture("query-8", "cn=userB, c=us", "SYSTEM-01", "TLDQueryLogic");

        // Counts towards totals for user and system.
        builder.capture("query-9", "cn=userA, c=us", "SYSTEM-01", "OtherQueryLogic");
        builder.capture("query-10", "cn=userA, c=us", "SYSTEM-01", "OtherQueryLogic");

        // Counts towards totals for system.
        builder.capture("query-11", "cn=userB, c=us", "SYSTEM-01", "OtherQueryLogic");
        builder.capture("query-12", "cn=userB, c=us", "SYSTEM-01", "OtherQueryLogic");

        builder.capture("query-13", "cn=userB, c=us", "SYSTEM-01", "EventQueryLogic");
        builder.capture("query-14", "cn=userB, c=us", "SYSTEM-01", "EventQueryLogic");

        // Counts towards totals for user and query logic.
        builder.capture("query-15", "cn=userA, c=us", "SYSTEM-02", "TLDQueryLogic");
        builder.capture("query-16", "cn=userA, c=us", "SYSTEM-02", "TLDQueryLogic");
        builder.capture("query-17", "cn=userA, c=us", "SYSTEM-02", "TLDQueryLogic");
        builder.capture("query-18", "cn=userA, c=us", "SYSTEM-02", "TLDQueryLogic");

        // Counts towards totals for query logic.
        builder.capture("query-19", "cn=userB, c=us", "SYSTEM-02", "TLDQueryLogic");
        builder.capture("query-20", "cn=userB, c=us", "SYSTEM-02", "TLDQueryLogic");
        builder.capture("query-21", "cn=userB, c=us", "SYSTEM-02", "TLDQueryLogic");

        // Counts towards totals for user.
        builder.capture("query-22", "cn=userA, c=us", "SYSTEM-02", "OtherQueryLogic");
        builder.capture("query-23", "cn=userA, c=us", "SYSTEM-02", "OtherQueryLogic");

        builder.capture("query-24", "cn=userB, c=us", "SYSTEM-02", "EventQueryLogic");
        builder.capture("query-25", "cn=userB, c=us", "SYSTEM-02", "EventQueryLogic");
        builder.capture("query-26", "cn=userC, c=us", "SYSTEM-02", "EventQueryLogic");

        ActiveQuerySnapshot snapshot = builder.build();

        assertThat(snapshot.getUserDn()).isEqualTo("cn=userA, c=us");
        assertThat(snapshot.getSystem()).isEqualTo("SYSTEM-01");
        assertThat(snapshot.getQueryLogics()).isEqualTo(queryLogics);
        assertThat(snapshot.getUserQueriesPerSystem()).isEqualTo(Map.of("SYSTEM-01", 7, "SYSTEM-02", 6));
        assertThat(snapshot.getTotalSystemQueries()).isEqualTo(14);
        // @formatter:off
        assertThat(snapshot.getTotalUserQueriesPerSystemPerQueryLogic()).isEqualTo(Map.of(
                        "SYSTEM-01", Map.of("EventQueryLogic", 1, "OtherQueryLogic", 2, "TLDQueryLogic", 4),
                        "SYSTEM-02", Map.of("OtherQueryLogic", 2, "TLDQueryLogic", 4)));
        // @formatter:on
        assertThat(snapshot.getTotalSystemQueriesPerQueryLogic()).isEqualTo(Map.of("EventQueryLogic", 3, "OtherQueryLogic", 4, "TLDQueryLogic", 7));
    }
}
