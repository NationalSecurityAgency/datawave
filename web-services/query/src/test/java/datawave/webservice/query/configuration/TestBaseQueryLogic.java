package datawave.webservice.query.configuration;

import com.google.common.collect.Sets;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.EasyRoleManager;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.logic.RoleManager;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
public class TestBaseQueryLogic {
    
    @Mock
    BaseQueryLogic<Object> copy;
    
    @Mock
    Query query;
    
    @Test
    public void testConstructor_Copy() throws Exception {
        // Set expectations
        expect(this.copy.getMarkingFunctions()).andReturn(null);
        expect(this.copy.getResponseObjectFactory()).andReturn(null);
        expect(this.copy.getLogicName()).andReturn("logicName");
        expect(this.copy.getLogicDescription()).andReturn("logicDescription");
        expect(this.copy.getAuditType(null)).andReturn(Auditor.AuditType.ACTIVE);
        expect(this.copy.getTableName()).andReturn("tableName");
        expect(this.copy.getMaxResults()).andReturn(Long.MAX_VALUE);
        expect(this.copy.getMaxWork()).andReturn(10L);
        expect(this.copy.getMaxPageSize()).andReturn(25);
        expect(this.copy.getPageByteTrigger()).andReturn(1024L);
        expect(this.copy.getCollectQueryMetrics()).andReturn(false);
        expect(this.copy.getConnPoolName()).andReturn("connPool1");
        expect(this.copy.getBaseIteratorPriority()).andReturn(100);
        expect(this.copy.getPrincipal()).andReturn(null);
        RoleManager roleManager = new EasyRoleManager();
        expect(this.copy.getRoleManager()).andReturn(roleManager);
        expect(this.copy.getSelectorExtractor()).andReturn(null);
        expect(this.copy.getBypassAccumulo()).andReturn(false);
        expect(this.copy.getAccumuloPassword()).andReturn("");
        expect(this.copy.getResponseEnricherBuilder()).andReturn(null);
        
        // Run the test
        PowerMock.replayAll();
        BaseQueryLogic<Object> subject = new TestQueryLogic<>(this.copy);
        int result1 = subject.getMaxPageSize();
        long result2 = subject.getPageByteTrigger();
        TransformIterator result3 = subject.getTransformIterator(this.query);
        PowerMock.verifyAll();
        
        // Verify results
        assertEquals("Incorrect max page size", 25, result1);
        assertEquals("Incorrect page byte trigger", 1024L, result2);
        assertNotNull("Iterator should not be null", result3);
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
        PowerMock.resetAll();
        expect(query.getDnList()).andReturn(dns);
        expect(query.getDnList()).andReturn(null);
        expect(query.getDnList()).andReturn(Collections.emptyList());
        expect(query.findParameter("systemFrom")).andReturn(null).anyTimes();
        PowerMock.replayAll();
        for (long limit : limits) {
            assertEquals(limit, logic.getResultLimit(query));
        }
        PowerMock.verifyAll();
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
        PowerMock.resetAll();
        expect(query.findParameter("systemFrom")).andReturn(new QueryImpl.Parameter("systemFrom", "hoplark"));
        expect(query.findParameter("systemFrom")).andReturn(null);
        expect(query.findParameter("systemFrom")).andReturn(new QueryImpl.Parameter("systemFrom", ""));
        expect(query.getDnList()).andReturn(null).anyTimes();
        PowerMock.replayAll();
        for (long limit : limits) {
            assertEquals(limit, logic.getResultLimit(query));
        }
        PowerMock.verifyAll();
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
        PowerMock.resetAll();
        
        // first call, populated dn list and populated system from
        expect(query.getDnList()).andReturn(dns);
        expect(query.findParameter("systemFrom")).andReturn(new QueryImpl.Parameter("systemFrom", "hoplark"));
        
        // second call, populated dn list and empty system from
        expect(query.getDnList()).andReturn(dns);
        expect(query.findParameter("systemFrom")).andReturn(null);
        
        // third call, null dn list and populated system from
        expect(query.getDnList()).andReturn(null);
        expect(query.findParameter("systemFrom")).andReturn(new QueryImpl.Parameter("systemFrom", "hoplark"));
        
        PowerMock.replayAll();
        for (long limit : limits) {
            assertEquals(limit, logic.getResultLimit(query));
        }
        PowerMock.verifyAll();
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
        systemFromResultLimits.put("hoplark", 25L);
        assertGetResultsLimitsSystemFrom(logic, 25L, 1000L, 1000L);
        
        // Assert cases given systemFromResultLimits == non-empty map with single match of a larger limit. The matching limit should be returned when
        // applicable.
        systemFromResultLimits.clear();
        systemFromResultLimits.put("hoplark", 5000L);
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
        
        // Assert cases given dnResultLimits == non-empty map with no matches and a systemFromResults as a non empty map with no matches. The maxResults should
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
        systemFromResultLimits.put("hoplark", 50L);
        
        assertGetResultsLimitsDnPrecedence(dns, logic, 25L, 25L, 50L);
        
        // Assert cases given dnResultLimits == non-empty map with single match of a larger limit and a systemFromResults with a matching limit. The matching
        // limit should be returned when applicable.
        dnResultLimits.clear();
        dnResultLimits.put("dn=user", 5000L);
        
        systemFromResultLimits.clear();
        systemFromResultLimits.put("hoplark", 50L);
        assertGetResultsLimitsDnPrecedence(dns, logic, 5000L, 5000L, 50L);
        
        // Assert cases given dnResultLimits == non-empty map with multiple matches and a systemFromResults with a matching limit. The smallest matching limit
        // should be returned when applicable.
        dnResultLimits.clear();
        dnResultLimits.put("dn=user", 25L);
        dnResultLimits.put("dn=user chain 1", 50L);
        dnResultLimits.put("dn=user chain 2", 1L);
        
        systemFromResultLimits.clear();
        systemFromResultLimits.put("hoplark", 75L);
        
        assertGetResultsLimitsDnPrecedence(dns, logic, 1L, 1L, 75L);
        
        // Assert cases given a dnResultsLimit == non-empty map with no matches and a systemFromResults with a matching limit. The systemFrom results limit
        // should take effect.
        dnResultLimits.clear();
        dnResultLimits.put("dn=other user", 25L);
        
        systemFromResultLimits.clear();
        systemFromResultLimits.put("hoplark", 75L);
        
        assertGetResultsLimitsDnPrecedence(dns, logic, 75L, 1000L, 75L);
    }
    
    private class TestQueryLogic<T> extends BaseQueryLogic<T> {
        
        public TestQueryLogic() {
            super();
        }
        
        public TestQueryLogic(BaseQueryLogic<T> other) {
            super(other);
        }
        
        @SuppressWarnings("rawtypes")
        @Override
        public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set runtimeQueryAuthorizations) throws Exception {
            return null;
        }
        
        @Override
        public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
            // No op
        }
        
        @Override
        public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                        throws Exception {
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
        public Object clone() throws CloneNotSupportedException {
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
