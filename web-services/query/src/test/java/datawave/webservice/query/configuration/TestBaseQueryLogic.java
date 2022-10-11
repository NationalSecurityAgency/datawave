package datawave.webservice.query.configuration;

import com.google.common.collect.Sets;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.query.Query;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.EasyRoleManager;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.logic.RoleManager;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@Disabled
@ExtendWith(MockitoExtension.class)
public class TestBaseQueryLogic {
    
    @Mock
    BaseQueryLogic<Object> copy;
    
    @Mock
    Query query;
    
    @Test
    public void testConstructor_Copy() throws Exception {
        // Set expectations
        when(this.copy.getMarkingFunctions()).thenReturn(null);
        when(this.copy.getResponseObjectFactory()).thenReturn(null);
        when(this.copy.getLogicName()).thenReturn("logicName");
        when(this.copy.getLogicDescription()).thenReturn("logicDescription");
        when(this.copy.getAuditType(null)).thenReturn(Auditor.AuditType.ACTIVE);
        when(this.copy.getTableName()).thenReturn("tableName");
        when(this.copy.getMaxResults()).thenReturn(Long.MAX_VALUE);
        when(this.copy.getMaxWork()).thenReturn(10L);
        when(this.copy.getMaxPageSize()).thenReturn(25);
        when(this.copy.getPageByteTrigger()).thenReturn(1024L);
        when(this.copy.getCollectQueryMetrics()).thenReturn(false);
        when(this.copy.getConnPoolName()).thenReturn("connPool1");
        when(this.copy.getBaseIteratorPriority()).thenReturn(100);
        when(this.copy.getPrincipal()).thenReturn(null);
        RoleManager roleManager = new EasyRoleManager();
        when(this.copy.getRoleManager()).thenReturn(roleManager);
        when(this.copy.getSelectorExtractor()).thenReturn(null);
        when(this.copy.getBypassAccumulo()).thenReturn(false);
        when(this.copy.getResponseEnricherBuilder()).thenReturn(null);
        
        // Run the test
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
        public GenericQueryConfiguration initialize(Connector connection, Query settings, Set runtimeQueryAuthorizations) throws Exception {
            return null;
        }
        
        @Override
        public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
            // No op
        }
        
        @Override
        public String getPlan(Connector connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
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
