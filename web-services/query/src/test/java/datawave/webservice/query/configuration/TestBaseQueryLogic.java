package datawave.webservice.query.configuration;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashSet;
import java.util.Set;

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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

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
        expect(this.copy.getUndisplayedVisibilities()).andReturn(new HashSet<>());
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
    
    private class TestQueryLogic<T> extends BaseQueryLogic<T> {
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
