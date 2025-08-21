package datawave.webservice.query.limit;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ActiveQuerySnapshotTest {
    
    /**
     * Verify that when a timestamp is not provided, that it is set to the current time.
     */
    @Test
    void testDefaultTimestamp() {
        long before = System.currentTimeMillis();
        ActiveQuerySnapshot snapshot = ActiveQuerySnapshot.builder("cn=testuser, c=us", "SYSTEM-01", "TLDQueryLogic").build();
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
        
        ActiveQuerySnapshot snapshot = ActiveQuerySnapshot.builder("cn=testuser, c=us", "SYSTEM-01", "TLDQueryLogic").withTimestamp(timestamp).build();
        
        assertThat(snapshot.getTimestamp()).isEqualTo(timestamp);
    }
    
    /**
     * Verify that an empty snapshot is structured correctly.
     */
    @Test
    void testSnapshotWithNoQueries() {
        ActiveQuerySnapshot snapshot = ActiveQuerySnapshot.builder("cn=testuser, c=us", "SYSTEM-01", "TLDQueryLogic").build();
        
        assertThat(snapshot.getUserDn()).isEqualTo("cn=testuser, c=us");
        assertThat(snapshot.getSystemName()).isEqualTo("SYSTEM-01");
        assertThat(snapshot.getQueryLogic()).isEqualTo("TLDQueryLogic");
        assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(0);
        assertThat(snapshot.getTotalUserQueriesForQueryLogic()).isEqualTo(0);
        assertThat(snapshot.getTotalUserQueriesPerSystem()).isEmpty();
        assertThat(snapshot.getTotalSystemQueries()).isEqualTo(0);
        assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(0);
        assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(0);
    }
    
    /**
     * Verify that query totals after consuming a variety of entries results in correct counts.
     */
    @Test
    void testSnapshotWithConsumedQueries() {
        ActiveQuerySnapshot.Builder builder = ActiveQuerySnapshot.builder("cn=userA, c=us", "SYSTEM-01", "TLDQueryLogic");
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
        assertThat(snapshot.getSystemName()).isEqualTo("SYSTEM-01");
        assertThat(snapshot.getQueryLogic()).isEqualTo("TLDQueryLogic");
        assertThat(snapshot.getTotalUserQueriesOnSystem()).isEqualTo(6);
        assertThat(snapshot.getTotalUserQueriesForQueryLogic()).isEqualTo(8);
        assertThat(snapshot.getTotalUserQueriesPerSystem()).isEqualTo(Map.of("SYSTEM-01", 6, "SYSTEM-02", 6));
        assertThat(snapshot.getTotalSystemQueries()).isEqualTo(11);
        assertThat(snapshot.getTotalSystemQueriesForQueryLogic()).isEqualTo(7);
        assertThat(snapshot.getTotalQueriesForQueryLogic()).isEqualTo(14);
    }
}
