package datawave.query.tables.content;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.easymock.PowerMock.expectLastCall;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.geotools.util.Base64;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;

import com.google.common.collect.Sets;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.config.ContentQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;

public class ContentQueryLogicTest {
    private ContentQueryLogic contentQueryLogic;
    private ScannerFactory mockScannerFactory;
    private BatchScanner mockScanner;
    private GenericQueryConfiguration mockGenericConfig;
    private ContentQueryConfiguration mockContentConfig;
    @Mock
    Query query;

    @Before
    public void setup() throws TableNotFoundException {
        contentQueryLogic = new ContentQueryLogic();
        mockScannerFactory = mock(ScannerFactory.class);
        mockScanner = mock(BatchScanner.class);
        mockGenericConfig = mock(GenericQueryConfiguration.class);
        mockContentConfig = mock(ContentQueryConfiguration.class);

        contentQueryLogic.scannerFactory = mockScannerFactory;
        when(mockScannerFactory.newScanner(any(), any(), anyInt(), any())).thenReturn(mockScanner);
    }

    @Test
    public void setupQueryInvalidConfigurationThrowsException() {
        assertThrows(QueryException.class, () -> contentQueryLogic.setupQuery(mockGenericConfig));
    }

    @Test
    public void setupQueryValidConfigurationSetsUpScanner() throws Exception {
        contentQueryLogic.setupQuery(mockContentConfig);
        verify(mockScanner).setRanges(any());
    }

    @Test
    public void setupQueryWithViewNameSetsIteratorSetting() throws Exception {
        contentQueryLogic.viewName = "FOO";
        contentQueryLogic.setupQuery(mockContentConfig);
        verify(mockScanner).addScanIterator(any());
    }

    @Test
    public void setupQueryWithViewNameSetsIteratorSetting2() throws Exception {
        contentQueryLogic.viewName = "BAR";
        contentQueryLogic.setupQuery(mockContentConfig);
        verify(mockScanner).addScanIterator(any());
    }

    @Test
    public void setupQueryWithViewNameSetsIteratorSetting3() throws Exception {
        contentQueryLogic.viewName = "BAZ";
        contentQueryLogic.setupQuery(mockContentConfig);
        verify(mockScanner).addScanIterator(any());
    }

    @Test
    public void setupQueryTableNotFoundThrowsRuntimeException() throws Exception {
        when(mockScannerFactory.newScanner(any(), any(), anyInt(), any())).thenThrow(TableNotFoundException.class);
        assertThrows(RuntimeException.class, () -> contentQueryLogic.setupQuery(mockContentConfig));
    }

    @Test
    public void testConstructorCopy() throws Exception {
        // borrowed from TestBaseQueryLogic.java
        ContentQueryLogic subject = new TestContentQuery();
        int result1 = subject.getMaxPageSize();
        long result2 = subject.getPageByteTrigger();
        TransformIterator result3 = subject.getTransformIterator(this.query);
        verifyAll();

        // Verify results
        assertEquals("Incorrect max page size", 0, result1);
        assertEquals("Incorrect page byte trigger", 0, result2);
        assertNotNull("Iterator should not be null", result3);
    }

    @Test
    public void testContainsDnWithAccess() {
        // borrowed from TestBaseQueryLogic.java
        Set<String> dns = Sets.newHashSet("dn=user", "dn=user chain 1", "dn=user chain 2");
        ContentQueryLogic logic = new TestContentQuery();

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
    public void testDecodeViewParam() throws Exception {
        AccumuloClient mockClient = PowerMock.createMock(AccumuloClient.class);
        MarkingFunctions mockMarkingFunctions = PowerMock.createMock(MarkingFunctions.class);

        ContentQueryLogic logic = new ContentQueryLogic();
        logic.setMarkingFunctions(mockMarkingFunctions);
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        Authorizations auths = new Authorizations("A");

        Query settings = new QueryImpl();
        settings.setQuery("event:20241218_0/samplecsv/1.2.3");
        settings.addParameter(QueryParameters.DECODE_VIEW, "true");
        settings.setQueryAuthorizations("A");

        Key dataKey = new Key("20241218_0", "d", "samplecsv" + '\u0000' + "1.2.3" + '\u0000' + "someView", "A");
        Value viewValue = new Value(Base64.encodeBytes("my happy message".getBytes()));
        Map.Entry<Key,Value> entry = new AbstractMap.SimpleImmutableEntry<>(dataKey, viewValue);

        mockMarkingFunctions.translateFromColumnVisibilityForAuths(new ColumnVisibility("A"), auths);
        expectLastCall().andReturn(Map.of("A", "A")).anyTimes();

        replayAll();

        // test with decode view
        logic.initialize(mockClient, settings, Set.of(auths));
        QueryLogicTransformer<Map.Entry<Key,Value>,EventBase> transformer = logic.getTransformer(settings);
        EventBase base = transformer.transform(entry);

        assertEquals(1, base.getFields().size());
        DefaultField field = (DefaultField) base.getFields().get(0);
        assertEquals("my happy message", field.getTypedValue().getValue());
        assertEquals("xs:string", field.getTypedValue().getType());

        // test without decode view
        settings.removeParameter(QueryParameters.DECODE_VIEW);
        logic.initialize(mockClient, settings, Set.of(auths));
        transformer = logic.getTransformer(settings);
        base = transformer.transform(entry);

        assertEquals(1, base.getFields().size());
        field = (DefaultField) base.getFields().get(0);
        assertEquals("xs:base64Binary", field.getTypedValue().getType());

        verifyAll();
    }

    private class TestContentQuery extends ContentQueryLogic {
        // borrowed from TestBaseQueryLogic.java
        public TestContentQuery() {
            super();
        }

        public TestContentQuery(TestContentQuery other) {
            super(other);
        }

        public TestContentQuery(BaseQueryLogic<Object> copy) {}

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
