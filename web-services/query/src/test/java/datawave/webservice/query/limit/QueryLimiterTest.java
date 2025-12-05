package datawave.webservice.query.limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.easymock.TestSubject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test cases for testing the functionality of {@link QueryLimiter}.
 */
@ExtendWith(EasyMockExtension.class)
class QueryLimiterTest {

    private static final String userDn = "cn=testuser, c=us";
    private static final String system = "SYSTEM-01";
    private static final String queryLogic = "TLDQueryLogic";

    @TestSubject
    private final QueryLimiter limiter = new QueryLimiter();

    @Mock
    private ActiveQueryTracker tracker;

    @Mock
    private UserLimitProvider userLimitProvider;

    @Mock
    private SystemLimitProvider systemLimitProvider;

    @Mock
    private QueryLogicGroupLimitProvider queryLogicGroupLimitProvider;

    @Mock
    private ActiveQuerySnapshot snapshot;

    @Mock
    private HostnameProvider hostnameProvider;

    /**
     * Verify that a user that meets their configured max user query limit results in an exceeds limit response.
     */
    @Test
    void testExceedsConfiguredUserQueryLimit() throws Exception {
        expectHostname();
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        expectUserQueryLimit(UserQueryLimit.fromConfig(userDn, 100, new TreeSet<>()));

        expect(tracker.getSnapshot(userDn, system, queryLogic)).andReturn(snapshot);

        replayAll();

        QueryLimiterResponse response = limiter.checkForLimits(userDn, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("User 'cn=testuser, c=us' has reached max user query limit of 100");

        verifyAll();
    }

    /**
     * Verify that a user that meets their default max user query limit results in an exceeds limit response.
     */
    @Test
    void testExceedsDefaultUserQueryLimit() throws Exception {
        expectHostname();
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        expectUserQueryLimit(UserQueryLimit.fromDefaults(userDn, 100));

        expect(tracker.getSnapshot(userDn, system, queryLogic)).andReturn(snapshot);

        replayAll();

        QueryLimiterResponse response = limiter.checkForLimits(userDn, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("User 'cn=testuser, c=us' has reached max default user query limit of 100");

        verifyAll();
    }

    /**
     * Verify that a user that meets their max user query limit for an overridden query logic group limit results in an exceeds limit response.
     */
    @Test
    void testExceedsUserQueryLogicGroupQueryLimit() throws Exception {
        expectHostname();
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        SortedSet<MatchableLimit> groupLimits = new TreeSet<>();
        groupLimits.add(new MatchableLimit("TLD", 25));
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, groupLimits);
        expectUserQueryLimit(userQueryLimit);
        expectOverriddenQueryLogicLimit(userQueryLimit, QueryLogicGroupQueryLimit.fromConfig("TLD", 25), 25);

        expect(tracker.getSnapshot(userDn, system, queryLogic)).andReturn(snapshot);

        replayAll();

        QueryLimiterResponse response = limiter.checkForLimits(userDn, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("User 'cn=testuser, c=us' has reached max user query limit of 25 for query logic TLDQueryLogic");

        verifyAll();
    }

    /**
     * Verify that a system that meets their configured max query limit results in an exceeds limit response.
     */
    @Test
    void testExceedsConfiguredSystemQueryLimit() throws Exception {
        expectHostname();
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        SortedSet<MatchableLimit> groupLimits = new TreeSet<>();
        groupLimits.add(new MatchableLimit("TLD", 25));
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, groupLimits);
        expectUserQueryLimit(userQueryLimit);
        expectOverriddenQueryLogicLimit(userQueryLimit, QueryLogicGroupQueryLimit.fromConfig("TLD", 25), 20);
        expectTotalSystemQueries(SystemQueryLimit.fromConfig("SYSTEM-01", 100, true, new TreeSet<>()), 100);

        expect(tracker.getSnapshot(userDn, system, queryLogic)).andReturn(snapshot);

        replayAll();

        QueryLimiterResponse response = limiter.checkForLimits(userDn, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("System 'SYSTEM-01' has reached max system query limit of 100");

        verifyAll();
    }

    /**
     * Verify that a system that meets their default max query limit results in an exceeds limit response.
     */
    @Test
    void testExceedsDefaultSystemQueryLimit() throws Exception {
        expectHostname();
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        SortedSet<MatchableLimit> groupLimits = new TreeSet<>();
        groupLimits.add(new MatchableLimit("TLD", 25));
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, groupLimits);
        expectUserQueryLimit(userQueryLimit);
        expectOverriddenQueryLogicLimit(userQueryLimit, QueryLogicGroupQueryLimit.fromConfig("TLD", 25), 20);
        expectTotalSystemQueries(SystemQueryLimit.fromDefaults("SYSTEM-01", 1000), 1000);

        expect(tracker.getSnapshot(userDn, system, queryLogic)).andReturn(snapshot);

        replayAll();

        QueryLimiterResponse response = limiter.checkForLimits(userDn, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("System 'SYSTEM-01' has reached max default system query limit of 1000");

        verifyAll();
    }

    /**
     * Verify that a system that meets their max query limit for an overridden query logic group limit results in an exceeds limit response.
     */
    @Test
    void testExceedsSystemQueryLogicGroupQueryLimit() throws Exception {
        expectHostname();
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        SortedSet<MatchableLimit> groupLimits = new TreeSet<>();
        groupLimits.add(new MatchableLimit("TLD", 25));
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, groupLimits);
        expectUserQueryLimit(userQueryLimit);
        expectOverriddenQueryLogicLimit(userQueryLimit, QueryLogicGroupQueryLimit.fromConfig("TLD", 25), 20);
        groupLimits = new TreeSet<>();
        groupLimits.add(new MatchableLimit("TLD", 200));
        SystemQueryLimit systemQueryLimit = SystemQueryLimit.fromConfig("SYSTEM-01", 1000, true, groupLimits);
        expectTotalSystemQueries(systemQueryLimit, 500);
        expectOverriddenQueryLogicLimit(systemQueryLimit, QueryLogicGroupQueryLimit.fromConfig("TLD", 200), 200);

        expect(tracker.getSnapshot(userDn, system, queryLogic)).andReturn(snapshot);

        replayAll();

        QueryLimiterResponse response = limiter.checkForLimits(userDn, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("System 'SYSTEM-01' has reached max query limit of 200 for query logic TLDQueryLogic");

        verifyAll();
    }

    /**
     * Verify that meets the default user query limit for a query logic group that they do not override results in an exceeds limit response.
     */
    @Test
    void testExceedsDefaultQueryLogicGroupQueryLimit() throws Exception {
        expectHostname();
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, new TreeSet<>());
        expectUserQueryLimit(userQueryLimit);
        SortedSet<MatchableLimit> groupLimits = new TreeSet<>();
        groupLimits.add(new MatchableLimit("TLD", 200));
        SystemQueryLimit systemQueryLimit = SystemQueryLimit.fromConfig("SYSTEM-01", 1000, true, groupLimits);
        expectTotalSystemQueries(systemQueryLimit, 500);
        expectOverriddenQueryLogicLimit(systemQueryLimit, QueryLogicGroupQueryLimit.fromConfig("TLD", 200), 150);
        expectQueryLogicLimit(QueryLogicGroupQueryLimit.fromConfig("TLD", 25), 25);

        expect(tracker.getSnapshot(userDn, system, queryLogic)).andReturn(snapshot);

        replayAll();

        QueryLimiterResponse response = limiter.checkForLimits(userDn, queryLogic);
        assertThat(response.exceedsLimit()).isTrue();
        assertThat(response.getMessage()).isEqualTo("User 'cn=testuser, c=us' has reached max default query limit of 25 for query logic TLDQueryLogic");

        verifyAll();
    }

    /**
     * Verify that when no limits are exceeded, the response reflects that.
     */
    @Test
    void testNoLimitsExceeded() throws Exception {
        expectHostname();
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, new TreeSet<>());
        expectUserQueryLimit(userQueryLimit);
        SortedSet<MatchableLimit> groupLimits = new TreeSet<>();
        groupLimits.add(new MatchableLimit("TLD", 200));
        SystemQueryLimit systemQueryLimit = SystemQueryLimit.fromConfig("SYSTEM-01", 1000, true, groupLimits);
        expectTotalSystemQueries(systemQueryLimit, 500);
        expectOverriddenQueryLogicLimit(systemQueryLimit, QueryLogicGroupQueryLimit.fromConfig("TLD", 200), 150);
        expectQueryLogicLimit(QueryLogicGroupQueryLimit.fromConfig("TLD", 25), 15);

        expect(tracker.getSnapshot(userDn, system, queryLogic)).andReturn(snapshot);

        replayAll();

        QueryLimiterResponse response = limiter.checkForLimits(userDn, queryLogic);
        assertThat(response.exceedsLimit()).isFalse();
        assertThat(response.getMessage()).isNull();

        verifyAll();
    }

    /**
     * Verify that when no matching query logic group limits are found, no issues occur.
     */
    @Test
    void testNoQueryLogicGroupLimitsFound() throws Exception {
        expectHostname();
        expectTotalUserQueries(Map.of(system, 100), Set.of());
        UserQueryLimit userQueryLimit = UserQueryLimit.fromConfig(userDn, 200, new TreeSet<>());
        expectUserQueryLimit(userQueryLimit);
        SortedSet<MatchableLimit> groupLimits = new TreeSet<>();
        groupLimits.add(new MatchableLimit("TLD", 200));
        SystemQueryLimit systemQueryLimit = SystemQueryLimit.fromConfig("SYSTEM-01", 1000, true, groupLimits);
        expectTotalSystemQueries(systemQueryLimit, 500);
        expectOverriddenQueryLogicLimit(systemQueryLimit, null, 150);
        expectQueryLogicLimit(null, 15);

        expect(tracker.getSnapshot(userDn, system, queryLogic)).andReturn(snapshot);

        replayAll();

        QueryLimiterResponse response = limiter.checkForLimits(userDn, queryLogic);
        assertThat(response.exceedsLimit()).isFalse();
        assertThat(response.getMessage()).isNull();

        verifyAll();
    }

    /**
     * Verify that tracking a query is delegated to the inner {@link ActiveQueryTracker}.
     */
    @Test
    void testTrackingQuery() throws Exception {
        tracker.trackQuery("queryId", "userDn", "system", "queryLogic");
        expectLastCall();

        replayAll();

        limiter.trackQuery("queryId", "userDn", "system", "queryLogic");

        verifyAll();
    }

    /**
     * Verify that stopping the tracking of a query is delegated to the inner {@link ActiveQueryTracker}.
     */
    @Test
    void testStopTrackingQuery() throws Exception {
        tracker.stopTrackingQuery("queryId");
        expectLastCall();

        replayAll();

        limiter.stopTrackingQuery("queryId");

        verifyAll();
    }

    /**
     * Verify that creating a heartbeat is delegated to the inner {@link ActiveQueryTracker}.
     */
    @Test
    void testCreatingHeartbeat() throws Exception {
        QueryHeartbeat heartbeat = mock(QueryHeartbeat.class);
        expect(tracker.createHeartbeat("queryId")).andReturn(heartbeat);

        replayAll();

        QueryHeartbeat actual = limiter.createHeartbeat("queryId");

        verifyAll();
        assertThat(actual).isEqualTo(heartbeat);
    }

    private void expectTotalUserQueries(Map<String,Integer> userQueriesPerSystem, Set<String> systemsNotCountingTowardsUserLimits) {
        expect(snapshot.getTotalUserQueriesPerSystem()).andReturn(userQueriesPerSystem);
        for (String system : userQueriesPerSystem.keySet()) {
            expect(systemLimitProvider.countsAgainstUserLimit(system)).andReturn(!systemsNotCountingTowardsUserLimits.contains(system));
        }
    }

    private void expectUserQueryLimit(UserQueryLimit userQueryLimit) {
        expect(userLimitProvider.getLimit(userDn)).andReturn(userQueryLimit);
    }

    private void expectOverriddenQueryLogicLimit(UserQueryLimit userQueryLimit, QueryLogicGroupQueryLimit groupLimit, int totalUserQueriesForQueryLogic) {
        Optional<QueryLogicGroupQueryLimit> optional = Optional.ofNullable(groupLimit);
        expect(queryLogicGroupLimitProvider.getOverriddenLimit(userDn, queryLogic, userQueryLimit.getQueryLogicGroupLimits())).andReturn(optional);
        if (optional.isPresent()) {
            expect(snapshot.getTotalUserQueriesForQueryLogic()).andReturn(totalUserQueriesForQueryLogic);
        }
    }

    private void expectTotalSystemQueries(SystemQueryLimit systemQueryLimit, int totalSystemQueries) {
        expect(systemLimitProvider.getLimit(system)).andReturn(systemQueryLimit);
        expect(snapshot.getTotalSystemQueries()).andReturn(totalSystemQueries);
    }

    private void expectOverriddenQueryLogicLimit(SystemQueryLimit userQueryLimit, QueryLogicGroupQueryLimit groupLimit, int totalSystemQueriesForQueryLogic) {
        Optional<QueryLogicGroupQueryLimit> optional = Optional.ofNullable(groupLimit);
        expect(queryLogicGroupLimitProvider.getOverriddenLimit(system, queryLogic, userQueryLimit.getQueryLogicGroupLimits())).andReturn(optional);
        if (optional.isPresent()) {
            expect(snapshot.getTotalSystemQueriesForQueryLogic()).andReturn(totalSystemQueriesForQueryLogic);
        }
    }

    private void expectQueryLogicLimit(QueryLogicGroupQueryLimit groupLimit, int totalUserQueriesForQueryLogic) {
        Optional<QueryLogicGroupQueryLimit> optional = Optional.ofNullable(groupLimit);
        expect(queryLogicGroupLimitProvider.getLimit(queryLogic)).andReturn(optional);
        if (optional.isPresent()) {
            expect(snapshot.getTotalUserQueriesForQueryLogic()).andReturn(totalUserQueriesForQueryLogic);
        }
    }

    private void expectHostname() throws UnknownHostException {
        expect(hostnameProvider.getCanonicalHostname()).andReturn(system);
    }

    private void replayAll() {
        replay(tracker, userLimitProvider, systemLimitProvider, queryLogicGroupLimitProvider, snapshot, hostnameProvider);
    }

    private void verifyAll() {
        verify(tracker, userLimitProvider, systemLimitProvider, queryLogicGroupLimitProvider, snapshot, hostnameProvider);
    }
}
