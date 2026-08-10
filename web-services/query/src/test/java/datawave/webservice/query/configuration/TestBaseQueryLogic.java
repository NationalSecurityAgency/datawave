package datawave.webservice.query.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import datawave.core.common.connection.AccumuloConnectionFactory.Priority;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.common.audit.Auditor;

public class TestBaseQueryLogic {

    private final BaseQueryLogic<Object> copy = mock(BaseQueryLogic.class);
    private final Query query = mock(Query.class);
    private final GenericQueryConfiguration config = mock(GenericQueryConfiguration.class);

    private final String SYSTEM_FROM = "SystemFrom";

    @Test
    public void testConstructor_Copy() {
        // Set expectations
        when(this.copy.getMarkingFunctions()).thenReturn(null);
        when(this.copy.getResponseObjectFactory()).thenReturn(null);
        when(this.copy.getLogicName()).thenReturn("logicName");
        when(this.copy.getLogicDescription()).thenReturn("logicDescription");
        when(this.copy.getAuditType(null)).thenReturn(Auditor.AuditType.ACTIVE);
        when(this.copy.getMaxPageSize()).thenReturn(25);
        when(this.copy.getPageByteTrigger()).thenReturn(1024L);
        when(this.copy.getCollectQueryMetrics()).thenReturn(false);
        when(this.copy.getRequiredRoles()).thenReturn(null);
        when(this.copy.getSelectorExtractor()).thenReturn(null);
        when(this.copy.getCurrentUser()).thenReturn(null);
        when(this.copy.getServerUser()).thenReturn(null);
        when(this.copy.getResponseEnricherBuilder()).thenReturn(null);
        ProxiedUserDetails principal = new DatawavePrincipal();
        when(this.copy.getCurrentUser()).thenReturn(principal);

        // setup expectations for GenericQueryConfig
        when(config.getQuery()).thenReturn(new QueryImpl());
        when(config.isCheckpointable()).thenReturn(false);
        when(config.getAuthorizations()).thenReturn(null);
        when(config.getQueryString()).thenReturn("FOO == 'bar'");
        when(config.getBeginDate()).thenReturn(null);
        when(config.getEndDate()).thenReturn(null);
        when(config.getMaxWork()).thenReturn(1L);
        when(config.getBaseIteratorPriority()).thenReturn(100);
        when(config.getTableName()).thenReturn("tableName");
        when(config.getBypassAccumulo()).thenReturn(false);
        when(config.getAccumuloPassword()).thenReturn("env:PASS");
        when(config.isReduceResults()).thenReturn(false);
        when(config.getClient()).thenReturn(null);
        when(config.getQueries()).thenReturn(Collections.emptyList());
        when(config.getQueriesIter()).thenReturn(Collections.emptyIterator());
        when(config.getTableConsistencyLevels()).thenReturn(Collections.emptyMap());
        when(config.getTableHints()).thenReturn(Collections.emptyMap());
        when(config.getConnPoolName()).thenReturn("connPool1");
        when(this.copy.getConfig()).thenReturn(config);

        BaseQueryLogic<Object> subject = new TestQueryLogic<>(this.copy);
        int result1 = subject.getMaxPageSize();
        long result2 = subject.getPageByteTrigger();
        TransformIterator result3 = subject.getTransformIterator(this.query);

        // Verify results
        assertEquals(25, result1, "Incorrect max page size");
        assertEquals(1024L, result2, "Incorrect page byte trigger");
        assertNotNull(result3, "Iterator should not be null");
    }

    @Test
    public void testContainsDnWithAccess() {
        Set<String> dns = Sets.newHashSet("dn=user", "dn=user chain 1", "dn=user chain 2");
        BaseQueryLogic<Object> logic = new TestQueryLogic<>();

        // Assert cases given allowedDNs == null. Access should not be blocked at all.
        assertTrue(logic.containsDNWithAccess(dns));
        assertTrue(logic.containsDNWithAccess(null));
        assertTrue(logic.containsDNWithAccess(Collections.emptySet()));

        // Assert cases given allowedDNs == empty set. Access should not be blocked at all.
        logic.setAuthorizedDNs(Collections.emptySet());
        assertTrue(logic.containsDNWithAccess(dns));
        assertTrue(logic.containsDNWithAccess(null));
        assertTrue(logic.containsDNWithAccess(Collections.emptySet()));

        // Assert cases given allowedDNs == non-empty set with matching DN. Access should only be granted where DN is present.
        logic.setAuthorizedDNs(Sets.newHashSet("dn=user", "dn=other user"));
        assertTrue(logic.containsDNWithAccess(dns));
        assertFalse(logic.containsDNWithAccess(null));
        assertFalse(logic.containsDNWithAccess(Collections.emptySet()));

        // Assert cases given allowedDNs == non-empty set with no matching DN. All access should be blocked.
        logic.setAuthorizedDNs(Sets.newHashSet("dn=other user", "dn=other user chain"));
        assertFalse(logic.containsDNWithAccess(dns));
        assertFalse(logic.containsDNWithAccess(null));
        assertFalse(logic.containsDNWithAccess(Collections.emptySet()));
    }

    /**
     * Sets up expectations and asserts the results from 3 types of query dn list with an empty system from in each case
     *
     * @param dns
     *            a list of dns to return from the first test permutation
     * @param logic
     *            the logic we will be testing
     * @param limits
     *            the expected limits to be returned in each case
     */
    public void assertGetResultsLimitsDn(List<String> dns, BaseQueryLogic<Object> logic, long... limits) {
        //  SystemFrom is always null
        when(query.getSystemFrom()).thenReturn(null);

        long[] expected = Arrays.copyOf(limits, limits.length);
        assertEquals(3, expected.length);

        when(query.getDnList()).thenReturn(dns);
        assertEquals(expected[0], logic.getResultLimit(query));

        when(query.getDnList()).thenReturn(null);
        assertEquals(expected[1], logic.getResultLimit(query));

        when(query.getDnList()).thenReturn(Collections.emptyList());
        assertEquals(expected[2], logic.getResultLimit(query));
    }

    /**
     * Sets up expectations and asserts the results from 3 types of query system from lists with an empty dn list in each case
     *
     * @param logic
     *            the logic we will be testing
     * @param limits
     *            the expected limits to be returned in each case
     */
    public void assertGetResultsLimitsSystemFrom(BaseQueryLogic<Object> logic, long... limits) {
        //  DN list is always null
        when(query.getDnList()).thenReturn(null);

        long[] expected = Arrays.copyOf(limits, limits.length);
        assertEquals(3, expected.length);

        when(query.getSystemFrom()).thenReturn(SYSTEM_FROM);
        assertEquals(expected[0], logic.getResultLimit(query));

        when(query.getSystemFrom()).thenReturn(null);
        assertEquals(expected[1], logic.getResultLimit(query));

        when(query.getSystemFrom()).thenReturn("");
        assertEquals(expected[2], logic.getResultLimit(query));
    }

    /**
     * Sets up expectations and asserts the results from 3 pairs of system from parameters and dn lists to ensure that the dn list always takes precedence when
     * present.
     *
     * @param logic
     *            the logic we will be testing
     * @param limits
     *            the expected limits to be returned in each case
     */
    public void assertGetResultsLimitsDnPrecedence(List<String> dns, BaseQueryLogic<Object> logic, long... limits) {

        long[] expected = Arrays.copyOf(limits, limits.length);
        assertEquals(3, expected.length);

        // first call, populated dn list and populated system from
        when(query.getDnList()).thenReturn(dns);
        when(query.getSystemFrom()).thenReturn(SYSTEM_FROM);
        assertEquals(expected[0], logic.getResultLimit(query));

        // second call, populated dn list and empty system from
        when(query.getDnList()).thenReturn(dns);
        when(query.getSystemFrom()).thenReturn(null);
        assertEquals(expected[1], logic.getResultLimit(query));

        // third call, null dn list and populated system from
        when(query.getDnList()).thenReturn(null);
        when(query.getSystemFrom()).thenReturn(SYSTEM_FROM);
        assertEquals(expected[2], logic.getResultLimit(query));
    }

    @Test
    public void testGetResultLimitDn() {
        List<String> dns = Arrays.asList("dn=user", "dn=user chain 1", "dn=user chain 2");
        BaseQueryLogic<Object> logic = new TestQueryLogic<>();
        logic.setMaxResults(1000L);

        // Assert cases given dnResultLimits == null. The maxResults should be returned.
        assertGetResultsLimitsDn(dns, logic, 1000L, 1000L, 1000L);

        // Assert cases given dnResultLimits == empty map. The maxResults should be returned.
        logic.setDnResultLimits(Collections.emptyMap());
        assertGetResultsLimitsDn(dns, logic, 1000L, 1000L, 1000L);

        // Assert cases given dnResultLimits == non-empty map with no matches. The maxResults should be returned.
        Map<String,Long> dnResultLimits = new HashMap<>();
        dnResultLimits.put("dn=other user", 25L);
        logic.setDnResultLimits(dnResultLimits);
        assertGetResultsLimitsDn(dns, logic, 1000L, 1000L, 1000L);

        // Assert cases given dnResultLimits == non-empty map with single match of a smaller limit. The matching limit should be returned when applicable.
        dnResultLimits.clear();
        dnResultLimits.put("dn=user", 25L);
        assertGetResultsLimitsDn(dns, logic, 25L, 1000L, 1000L);

        // Assert cases given dnResultLimits == non-empty map with single match of a larger limit. The matching limit should be returned when applicable.
        dnResultLimits.clear();
        dnResultLimits.put("dn=user", 5000L);
        assertGetResultsLimitsDn(dns, logic, 5000L, 1000L, 1000L);

        // Assert cases given dnResultLimits == non-empty map with multiple matches. The smallest matching limit should be returned when applicable.
        dnResultLimits.clear();
        dnResultLimits.put("dn=user", 25L);
        dnResultLimits.put("dn=user chain 1", 50L);
        dnResultLimits.put("dn=user chain 2", 1L);
        assertGetResultsLimitsDn(dns, logic, 1L, 1000L, 1000L);
    }

    @Test
    public void testGetResultLimitSystemFrom() {
        BaseQueryLogic<Object> logic = new TestQueryLogic<>();
        logic.setMaxResults(1000L);

        // Assert cases given systemFromResultLimits == null. The maxResults should be returned.
        assertGetResultsLimitsSystemFrom(logic, 1000L, 1000L, 1000L);

        // Assert cases given systemFromResultLimits == empty map. The maxResults should be returned.
        logic.setSystemFromResultLimits(Collections.emptyMap());
        assertGetResultsLimitsSystemFrom(logic, 1000L, 1000L, 1000L);

        // Assert cases given systemFromResultLimits == non-empty map with no matches. The maxResults should be returned.
        Map<String,Long> systemFromResultLimits = new HashMap<>();
        systemFromResultLimits.put("someOtherSystem", 25L);
        logic.setSystemFromResultLimits(systemFromResultLimits);
        assertGetResultsLimitsSystemFrom(logic, 1000L, 1000L, 1000L);

        // Assert cases given systemFromResultLimits == non-empty map with single match of a smaller limit. The matching limit should be returned when
        // applicable.
        systemFromResultLimits.clear();
        systemFromResultLimits.put(SYSTEM_FROM, 25L);
        assertGetResultsLimitsSystemFrom(logic, 25L, 1000L, 1000L);

        // Assert cases given systemFromResultLimits == non-empty map with single match of a larger limit. The matching limit should be returned when
        // applicable.
        systemFromResultLimits.clear();
        systemFromResultLimits.put(SYSTEM_FROM, 5000L);
        assertGetResultsLimitsSystemFrom(logic, 5000L, 1000L, 1000L);
    }

    @Test
    public void testGetResultLimitDnPrecedence() {
        List<String> dns = Arrays.asList("dn=user", "dn=user chain 1", "dn=user chain 2");
        BaseQueryLogic<Object> logic = new TestQueryLogic<>();
        logic.setMaxResults(1000L);

        // Assert cases given dnResultLimits == null and systemFromResults = null The maxResults should be returned.
        assertGetResultsLimitsDnPrecedence(dns, logic, 1000L, 1000L, 1000L);

        // Assert cases given dnResultLimits == empty map and systemFromResults == empty map. The maxResults should be returned.
        logic.setDnResultLimits(Collections.emptyMap());
        logic.setSystemFromResultLimits(Collections.emptyMap());
        assertGetResultsLimitsDnPrecedence(dns, logic, 1000L, 1000L, 1000L);

        // Assert cases given dnResultLimits == non-empty map with no matches and a systemFromResults as a non-empty map with no matches. The maxResults should
        // be returned.
        Map<String,Long> dnResultLimits = new HashMap<>();
        Map<String,Long> systemFromResultLimits = new HashMap<>();

        dnResultLimits.put("dn=other user", 25L);
        systemFromResultLimits.put("someOtherSystem", 50L);

        logic.setDnResultLimits(dnResultLimits);
        logic.setSystemFromResultLimits(systemFromResultLimits);

        assertGetResultsLimitsDnPrecedence(dns, logic, 1000L, 1000L, 1000L);

        // Assert cases given dnResultLimits == non-empty map with single match of a smaller limit and a systemFromResults with a matching limit. The matching
        // limit should be returned when applicable.
        dnResultLimits.clear();
        dnResultLimits.put("dn=user", 25L);

        systemFromResultLimits.clear();
        systemFromResultLimits.put(SYSTEM_FROM, 50L);

        assertGetResultsLimitsDnPrecedence(dns, logic, 25L, 25L, 50L);

        // Assert cases given dnResultLimits == non-empty map with single match of a larger limit and a systemFromResults with a matching limit. The matching
        // limit should be returned when applicable.
        dnResultLimits.clear();
        dnResultLimits.put("dn=user", 5000L);

        systemFromResultLimits.clear();
        systemFromResultLimits.put(SYSTEM_FROM, 50L);
        assertGetResultsLimitsDnPrecedence(dns, logic, 5000L, 5000L, 50L);

        // Assert cases given dnResultLimits == non-empty map with multiple matches and a systemFromResults with a matching limit. The smallest matching limit
        // should be returned when applicable.
        dnResultLimits.clear();
        dnResultLimits.put("dn=user", 25L);
        dnResultLimits.put("dn=user chain 1", 50L);
        dnResultLimits.put("dn=user chain 2", 1L);

        systemFromResultLimits.clear();
        systemFromResultLimits.put(SYSTEM_FROM, 75L);

        assertGetResultsLimitsDnPrecedence(dns, logic, 1L, 1L, 75L);

        // Assert cases given a dnResultsLimit == non-empty map with no matches and a systemFromResults with a matching limit. The systemFrom results limit
        // should take effect.
        dnResultLimits.clear();
        dnResultLimits.put("dn=other user", 25L);

        systemFromResultLimits.clear();
        systemFromResultLimits.put(SYSTEM_FROM, 75L);

        assertGetResultsLimitsDnPrecedence(dns, logic, 75L, 1000L, 75L);
    }

    private static class TestQueryLogic<T> extends BaseQueryLogic<T> {

        public TestQueryLogic() {
            super();
        }

        public TestQueryLogic(BaseQueryLogic<T> other) {
            super(other);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set runtimeQueryAuthorizations) {
            return null;
        }

        @Override
        public void setupQuery(GenericQueryConfiguration configuration) {
            // No op
        }

        @Override
        public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields,
                        boolean expandValues) {
            return "";
        }

        @Override
        public Priority getConnectionPriority() {
            return null;
        }

        @Override
        public QueryLogicTransformer getTransformer(Query settings) {
            return null;
        }

        @Override
        public Object clone() {
            return null;
        }

        @Override
        public Set<String> getOptionalQueryParameters() {
            return null;
        }

        @Override
        public Set<String> getRequiredQueryParameters() {
            return null;
        }

        @Override
        public Set<String> getExampleQueries() {
            // TODO Auto-generated method stub
            return null;
        }

    }
}
