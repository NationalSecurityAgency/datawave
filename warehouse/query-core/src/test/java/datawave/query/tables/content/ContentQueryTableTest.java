package datawave.query.tables.content;

import com.google.common.collect.Sets;
import datawave.query.config.ContentQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.powermock.api.easymock.PowerMock;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ContentQueryTableTest {
    private ContentQueryTable contentQueryTable;
    private ScannerFactory mockScannerFactory;
    private BatchScanner mockScanner;
    private GenericQueryConfiguration mockGenericConfig;
    private ContentQueryConfiguration mockContentConfig;
    @org.powermock.api.easymock.annotation.Mock
    Query query;
    
    @Before
    public void setup() throws TableNotFoundException {
        contentQueryTable = new ContentQueryTable();
        mockScannerFactory = mock(ScannerFactory.class);
        mockScanner = mock(BatchScanner.class);
        mockGenericConfig = mock(GenericQueryConfiguration.class);
        mockContentConfig = mock(ContentQueryConfiguration.class);
        
        contentQueryTable.scannerFactory = mockScannerFactory;
        when(mockScannerFactory.newScanner(any(), any(), anyInt(), any())).thenReturn(mockScanner);
    }
    
    @Test
    public void setupQueryInvalidConfigurationThrowsException() {
        assertThrows(QueryException.class, () -> contentQueryTable.setupQuery(mockGenericConfig));
    }
    
    @Test
    public void setupQueryValidConfigurationSetsUpScanner() throws Exception {
        contentQueryTable.setupQuery(mockContentConfig);
        verify(mockScanner).setRanges(any());
    }
    
    @Test
    public void setupQueryWithViewNameSetsIteratorSetting() throws Exception {
        contentQueryTable.viewName = "FOO";
        contentQueryTable.setupQuery(mockContentConfig);
        verify(mockScanner).addScanIterator(any());
    }
    
    @Test
    public void setupQueryWithViewNameSetsIteratorSetting2() throws Exception {
        contentQueryTable.viewName = "BAR";
        contentQueryTable.setupQuery(mockContentConfig);
        verify(mockScanner).addScanIterator(any());
    }
    
    @Test
    public void setupQueryWithViewNameSetsIteratorSetting3() throws Exception {
        contentQueryTable.viewName = "BAZ";
        contentQueryTable.setupQuery(mockContentConfig);
        verify(mockScanner).addScanIterator(any());
    }
    
    @Test
    public void setupQueryTableNotFoundThrowsRuntimeException() throws Exception {
        when(mockScannerFactory.newScanner(any(), any(), anyInt(), any())).thenThrow(TableNotFoundException.class);
        assertThrows(RuntimeException.class, () -> contentQueryTable.setupQuery(mockContentConfig));
    }
    
    @Test
    public void testConstructorCopy() throws Exception {
        // borrowed from TestBaseQueryLogic.java
        ContentQueryTable subject = new TestContentQuery();
        int result1 = subject.getMaxPageSize();
        long result2 = subject.getPageByteTrigger();
        TransformIterator result3 = subject.getTransformIterator(this.query);
        PowerMock.verifyAll();
        
        // Verify results
        assertEquals("Incorrect max page size", 0, result1);
        assertEquals("Incorrect page byte trigger", 0, result2);
        assertNotNull("Iterator should not be null", result3);
    }
    
    @Test
    public void testContainsDnWithAccess() {
        // borrowed from TestBaseQueryLogic.java
        Set<String> dns = Sets.newHashSet("dn=user", "dn=user chain 1", "dn=user chain 2");
        ContentQueryTable logic = new TestContentQuery();
        
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
    
    private class TestContentQuery extends ContentQueryTable {
        // borrowed from TestBaseQueryLogic.java
        public TestContentQuery() {
            super();
        }
        
        public TestContentQuery(TestContentQuery other) {
            super(other);
        }
        
        public TestContentQuery(BaseQueryLogic<Object> copy) {}
        
        @Override
        public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
            // No op
        }
        
        @Override
        public AccumuloConnectionFactory.Priority getConnectionPriority() {
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
            return null;
        }
    }
}
