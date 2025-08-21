package datawave.webservice.query.limit;

import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.easymock.TestSubject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

@ExtendWith(EasyMockExtension.class)
class QueryLimiterTest {
    
    private static final String userDn = "cn=testuser, c=us";
    private static final String system = "SYSTEM-01";
    private static final String queryLogic = "TLDQueryLogic";
    
    @TestSubject
    private final QueryLimiter limiter = new QueryLimiter();
    
    @Mock
    private QueryLimiter.SnapshotProvider snapshotProvider;
    
    @Mock
    private QueryLimitProvider limitProvider;
    
    @Mock
    private ActiveQuerySnapshot snapshot;
    
    @BeforeEach
    void setUp() throws Exception {
        expect(snapshotProvider.getSnapshot(userDn, system, queryLogic)).andReturn(snapshot);
    }
    
    /**
     * Verify that a user that meets their configured max user query limit results in an exceeds limit response.
     */
    @Test
    void testExceedsConfiguredUserQueryLimit() throws Exception {
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        expectUserQueryLimit(UserQueryLimit.fromConfig(userDn, 100, Map.of()));
        
        replay(snapshotProvider, limitProvider, snapshot);
        
        QueryLimiterResponse response = limiter.checkForLimits(userDn, system, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("User 'cn=testuser, c=us' has reached max user query limit of 100");
    }
    
    /**
     * Verify that a user that meets their default max user query limit results in an exceeds limit response.
     */
    @Test
    void testExceedsDefaultUserQueryLimit() throws Exception {
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        expectUserQueryLimit(UserQueryLimit.fromDefaults(userDn, 100));
        
        replay(snapshotProvider, limitProvider, snapshot);
        
        QueryLimiterResponse response = limiter.checkForLimits(userDn, system, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("User 'cn=testuser, c=us' has reached max default user query limit of 100");
    }
    
    /**
     * Verify that a user that meets their max user query limit for an overridden query logic group limit results in an exceeds limit response.
     */
    @Test
    void testExceedsUserQueryLogicGroupQueryLimit() throws Exception {
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, Map.of("TLD", 25));
        expectUserQueryLimit(userQueryLimit);
        expectOverriddenQueryLogicLimit(userQueryLimit, QueryLogicGroupLimit.fromConfig("TLD", 25), 25);
        
        replay(snapshotProvider, limitProvider, snapshot);
        
        QueryLimiterResponse response = limiter.checkForLimits(userDn, system, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("User 'cn=testuser, c=us' has reached max user query limit of 25 for query logic TLDQueryLogic");
    }
    
    /**
     * Verify that a system that meets their configured max query limit results in an exceeds limit response.
     */
    @Test
    void testExceedsConfiguredSystemQueryLimit() throws Exception {
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, Map.of("TLD", 25));
        expectUserQueryLimit(userQueryLimit);
        expectOverriddenQueryLogicLimit(userQueryLimit, QueryLogicGroupLimit.fromConfig("TLD", 25), 20);
        expectTotalSystemQueries(SystemQueryLimit.fromConfig("SYSTEM-01", 100, true, Map.of()), 100);
        
        replay(snapshotProvider, limitProvider, snapshot);
        
        QueryLimiterResponse response = limiter.checkForLimits(userDn, system, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("System 'SYSTEM-01' has reached max system query limit of 100");
    }
    
    /**
     * Verify that a system that meets their default max query limit results in an exceeds limit response.
     */
    @Test
    void testExceedsDefaultSystemQueryLimit() throws Exception {
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, Map.of("TLD", 25));
        expectUserQueryLimit(userQueryLimit);
        expectOverriddenQueryLogicLimit(userQueryLimit, QueryLogicGroupLimit.fromConfig("TLD", 25), 20);
        expectTotalSystemQueries(SystemQueryLimit.fromDefaults("SYSTEM-01", 1000), 1000);
        
        replay(snapshotProvider, limitProvider, snapshot);
        
        QueryLimiterResponse response = limiter.checkForLimits(userDn, system, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("System 'SYSTEM-01' has reached max default system query limit of 1000");
    }
    
    /**
     * Verify that a system that meets their max query limit for an overridden query logic group limit results in an exceeds limit response.
     */
    @Test
    void testExceedsSystemQueryLogicGroupQueryLimit() throws Exception {
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, Map.of("TLD", 25));
        expectUserQueryLimit(userQueryLimit);
        expectOverriddenQueryLogicLimit(userQueryLimit, QueryLogicGroupLimit.fromConfig("TLD", 25), 20);
        SystemQueryLimit systemQueryLimit = SystemQueryLimit.fromConfig("SYSTEM-01", 1000, true, Map.of("TLD", 200));
        expectTotalSystemQueries(systemQueryLimit, 500);
        expectOverriddenQueryLogicLimit(systemQueryLimit, QueryLogicGroupLimit.fromConfig("TLD", 200), 200);
        
        replay(snapshotProvider, limitProvider, snapshot);
        
        QueryLimiterResponse response = limiter.checkForLimits(userDn, system, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("System 'SYSTEM-01' has reached max query limit of 200 for query logic TLDQueryLogic");
    }
    
    /**
     * Verify that meets the default user query limit for a query logic group that they do not override results in an exceeds limit response.
     */
    @Test
    void testExceedsDefaultQueryLogicGroupQueryLimit() throws Exception {
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, Map.of());
        expectUserQueryLimit(userQueryLimit);
        expectOverriddenQueryLogicLimit(userQueryLimit, null, 25);
        SystemQueryLimit systemQueryLimit = SystemQueryLimit.fromConfig("SYSTEM-01", 1000, true, Map.of("TLD", 200));
        expectTotalSystemQueries(systemQueryLimit, 500);
        expectOverriddenQueryLogicLimit(systemQueryLimit, QueryLogicGroupLimit.fromConfig("TLD", 200), 150);
        expectQueryLogicLimit(QueryLogicGroupLimit.fromConfig("TLD", 25), 25);
        
        replay(snapshotProvider, limitProvider, snapshot);
        
        QueryLimiterResponse response = limiter.checkForLimits(userDn, system, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("User 'cn=testuser, c=us' has reached max default query limit of 25 for query logic TLDQueryLogic");
    }
    
    /**
     * Verify that when no limits are exceeded, the response reflects that.
     */
    @Test
    void testNoLimitsExceeded() throws Exception {
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, Map.of());
        expectUserQueryLimit(userQueryLimit);
        expectOverriddenQueryLogicLimit(userQueryLimit, null, 25);
        SystemQueryLimit systemQueryLimit = SystemQueryLimit.fromConfig("SYSTEM-01", 1000, true, Map.of("TLD", 200));
        expectTotalSystemQueries(systemQueryLimit, 500);
        expectOverriddenQueryLogicLimit(systemQueryLimit, QueryLogicGroupLimit.fromConfig("TLD", 200), 150);
        expectQueryLogicLimit(QueryLogicGroupLimit.fromConfig("TLD", 25), 15);
        
        replay(snapshotProvider, limitProvider, snapshot);
        
        QueryLimiterResponse response = limiter.checkForLimits(userDn, system, queryLogic);
        assertThat(response.exceedsLimit()).isFalse();
        assertThat(response.getMessage()).isNull();
    }
    
    /**
     * Verify that when no matching query logic group limits are found, no issues occur.
     */
    @Test
    void testNoQueryLogicGroupLimitsFound() throws Exception {
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, Map.of());
        expectUserQueryLimit(userQueryLimit);
        expectOverriddenQueryLogicLimit(userQueryLimit, null, 25);
        SystemQueryLimit systemQueryLimit = SystemQueryLimit.fromConfig("SYSTEM-01", 1000, true, Map.of("TLD", 200));
        expectTotalSystemQueries(systemQueryLimit, 500);
        expectOverriddenQueryLogicLimit(systemQueryLimit, null, 150);
        expectQueryLogicLimit(null, 15);
        
        replay(snapshotProvider, limitProvider, snapshot);
        
        QueryLimiterResponse response = limiter.checkForLimits(userDn, system, queryLogic);
        assertThat(response.exceedsLimit()).isFalse();
        assertThat(response.getMessage()).isNull();
    }
    
    private void expectTotalUserQueries(Map<String,Integer> userQueriesPerSystem, Set<String> systemsNotCountingTowardsUserLimits) {
        expect(snapshot.getTotalUserQueriesPerSystem()).andReturn(userQueriesPerSystem);
        for(String system : userQueriesPerSystem.keySet()) {
            expect(limitProvider.systemCountsAgainstUserLimit(system)).andReturn(!systemsNotCountingTowardsUserLimits.contains(system));
        }
    }
    
    private void expectUserQueryLimit(UserQueryLimit userQueryLimit) {
        expect(limitProvider.getUserLimit(userDn)).andReturn(userQueryLimit);
    }
    
    private void expectOverriddenQueryLogicLimit(UserQueryLimit userQueryLimit, QueryLogicGroupLimit groupLimit, int totalUserQueriesForQueryLogic) {
        Optional<QueryLogicGroupLimit> optional = Optional.ofNullable(groupLimit);
        expect(limitProvider.getOverriddenQueryLogicGroupLimit(userDn, queryLogic, userQueryLimit.getQueryLogicGroupLimits())).andReturn(optional);
        if(optional.isPresent()) {
            expect(snapshot.getTotalUserQueriesForQueryLogic()).andReturn(totalUserQueriesForQueryLogic);
        }
    }
    
    private void expectTotalSystemQueries(SystemQueryLimit systemQueryLimit, int totalSystemQueries) {
        expect(limitProvider.getSystemLimit(system)).andReturn(systemQueryLimit);
        expect(snapshot.getTotalSystemQueries()).andReturn(totalSystemQueries);
    }
    
    private void expectOverriddenQueryLogicLimit(SystemQueryLimit userQueryLimit, QueryLogicGroupLimit groupLimit, int totalSystemQueriesForQueryLogic) {
        Optional<QueryLogicGroupLimit> optional = Optional.ofNullable(groupLimit);
        expect(limitProvider.getOverriddenQueryLogicGroupLimit(system, queryLogic, userQueryLimit.getQueryLogicGroupLimits())).andReturn(optional);
        if(optional.isPresent()) {
            expect(snapshot.getTotalSystemQueriesForQueryLogic()).andReturn(totalSystemQueriesForQueryLogic);
        }
    }
    
    private void expectQueryLogicLimit(QueryLogicGroupLimit groupLimit, int totalUserQueriesForQueryLogic) {
        Optional<QueryLogicGroupLimit> optional = Optional.ofNullable(groupLimit);
        expect(limitProvider.getQueryLogicGroupLimit(queryLogic)).andReturn(optional);
        if(optional.isPresent()) {
            expect(snapshot.getTotalUserQueriesForQueryLogic()).andReturn(totalUserQueriesForQueryLogic);
        }
    }
}
