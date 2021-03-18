package datawave.webservice.query.configuration;

import com.google.common.collect.Sets;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.query.Query;
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

import java.util.Collections;
import java.util.HashMap;
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
    
    @Test
    public void testGetResultLimit() {
        Set<String> dns = Sets.newHashSet("dn=user", "dn=user chain 1", "dn=user chain 2");
        BaseQueryLogic<Object> logic = new TestQueryLogic<>();
        logic.setMaxResults(1000L);
        
        // Assert cases given dnResultLimits == null. The maxResults should be returned.
        assertEquals(1000L, logic.getResultLimit(dns));
        assertEquals(1000L, logic.getResultLimit(null));
        assertEquals(1000L, logic.getResultLimit(Collections.emptySet()));
        
        // Assert cases given dnResultLimits == empty map. The maxResults should be returned.
        logic.setDnResultLimits(Collections.emptyMap());
        assertEquals(1000L, logic.getResultLimit(dns));
        assertEquals(1000L, logic.getResultLimit(null));
        assertEquals(1000L, logic.getResultLimit(Collections.emptySet()));
        
        // Assert cases given dnResultLimits == non-empty map with no matches. The maxResults should be returned.
        Map<String,Long> dnResultLimits = new HashMap<>();
        dnResultLimits.put("dn=other user", 25L);
        logic.setDnResultLimits(dnResultLimits);
        assertEquals(1000L, logic.getResultLimit(dns));
        assertEquals(1000L, logic.getResultLimit(null));
        assertEquals(1000L, logic.getResultLimit(Collections.emptySet()));
        
        // Assert cases given dnResultLimits == non-empty map with single match of a smaller limit. The matching limit should be returned when applicable.
        dnResultLimits.clear();
        dnResultLimits.put("dn=user", 25L);
        assertEquals(25L, logic.getResultLimit(dns));
        assertEquals(1000L, logic.getResultLimit(null));
        assertEquals(1000L, logic.getResultLimit(Collections.emptySet()));
        
        // Assert cases given dnResultLimits == non-empty map with single match of a larger limit. The matching limit should be returned when applicable.
        dnResultLimits.clear();
        dnResultLimits.put("dn=user", 5000L);
        assertEquals(5000L, logic.getResultLimit(dns));
        assertEquals(1000L, logic.getResultLimit(null));
        assertEquals(1000L, logic.getResultLimit(Collections.emptySet()));
        
        // Assert cases given dnResultLimits == non-empty map with multiple matches. The smallest matching limit should be returned when applicable.
        dnResultLimits.clear();
        dnResultLimits.put("dn=user", 25L);
        dnResultLimits.put("dn=user chain 1", 50L);
        dnResultLimits.put("dn=user chain 2", 1L);
        assertEquals(1L, logic.getResultLimit(dns));
        assertEquals(1000L, logic.getResultLimit(null));
        assertEquals(1000L, logic.getResultLimit(Collections.emptySet()));
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
