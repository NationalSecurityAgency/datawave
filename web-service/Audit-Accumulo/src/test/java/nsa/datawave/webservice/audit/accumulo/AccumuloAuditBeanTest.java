package nsa.datawave.webservice.audit.accumulo;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.*;
import static org.powermock.api.support.membermodification.MemberMatcher.field;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.ws.rs.core.MultivaluedMap;

import nsa.datawave.webservice.common.audit.AuditParameters;
import nsa.datawave.webservice.common.audit.Auditor;
import nsa.datawave.webservice.common.audit.Auditor.AuditType;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import nsa.datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class AccumuloAuditBeanTest extends EasyMockSupport {
    private static final String TABLE_NAME = "QueryAuditTable";
    
    private AccumuloAuditBean audit;
    private AccumuloConnectionFactory conFactory;
    private Connector mockConnector;
    
    @Before
    public void setup() throws Exception {
        
        conFactory = createStrictMock(AccumuloConnectionFactory.class);
        InMemoryInstance InMemoryInstance = new InMemoryInstance("testInstance");
        mockConnector = InMemoryInstance.getConnector("root", new PasswordToken(""));
        
        audit = new AccumuloAuditBean();
        field(AccumuloAuditBean.class, "connectionFactory").set(audit, conFactory);
        
        if (mockConnector.tableOperations().exists(TABLE_NAME)) {
            mockConnector.tableOperations().delete(TABLE_NAME);
        }
        
    }
    
    @Test
    public void testInit() throws Exception {
        HashMap<String,String> trackingMap = new HashMap<>();
        expect(conFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        expect(conFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN), EasyMock.eq(trackingMap))).andReturn(mockConnector);
        conFactory.returnConnection(mockConnector);
        replayAll(); // replay all mock objects
        
        assertFalse(TABLE_NAME + " already exists before test", mockConnector.tableOperations().exists(TABLE_NAME));
        audit.init();
        
        verifyAll(); // verify all mock objects
        assertTrue(TABLE_NAME + " doesn't exist after test", mockConnector.tableOperations().exists(TABLE_NAME));
    }
    
    @Test
    public void testInitWithTableAlreadyThere() throws Exception {
        if (!mockConnector.tableOperations().exists(TABLE_NAME)) {
            mockConnector.tableOperations().create(TABLE_NAME);
        }
        
        HashMap<String,String> trackingMap = new HashMap<>();
        expect(conFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        expect(conFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN), EasyMock.eq(trackingMap))).andReturn(mockConnector);
        conFactory.returnConnection(mockConnector);
        replayAll(); // replay all mock objects
        
        assertTrue(TABLE_NAME + " doesn't exist before test", mockConnector.tableOperations().exists(TABLE_NAME));
        audit.init();
        
        verifyAll(); // verify all mock objects
        assertTrue(TABLE_NAME + " doesn't exist after test", mockConnector.tableOperations().exists(TABLE_NAME));
    }
    
    @Test
    public void testAudit() throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat(Auditor.ISO_8601_FORMAT_STRING);
        
        if (!mockConnector.tableOperations().exists(TABLE_NAME)) {
            mockConnector.tableOperations().create(TABLE_NAME);
        }
        HashMap<String,String> trackingMap = new HashMap<>();
        expect(conFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        expect(conFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN), EasyMock.eq(trackingMap))).andReturn(mockConnector);
        conFactory.returnConnection(mockConnector);
        expect(conFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        expect(conFactory.getConnection(EasyMock.eq(AccumuloConnectionFactory.Priority.ADMIN), EasyMock.eq(trackingMap))).andReturn(mockConnector);
        conFactory.returnConnection(mockConnector);
        replayAll(); // replay all mock objects
        
        audit.init();
        
        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        p.put(AuditParameters.USER_DN, Collections.singletonList("someUser"));
        p.put(AuditParameters.QUERY_AUTHORIZATIONS, Collections.singletonList("AUTH1,AUTH2"));
        p.put(AuditParameters.QUERY_STRING, Collections.singletonList("test query"));
        p.put(AuditParameters.QUERY_AUDIT_TYPE, Collections.singletonList(AuditType.ACTIVE.name()));
        p.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Collections.singletonList("ALL"));
        AuditParameters ap = new AuditParameters();
        ap.validate(p);
        Date date = ap.getQueryDate();
        audit.audit(ap);
        
        verifyAll(); // verify all mock objects
        
        Scanner scanner = mockConnector.createScanner(TABLE_NAME, new Authorizations("ALL"));
        Iterator<Entry<Key,Value>> it = scanner.iterator();
        assertTrue(it.hasNext());
        Entry<Key,Value> entry = it.next();
        Key key = entry.getKey();
        Value value = entry.getValue();
        assertEquals(formatter.format(date), key.getRow().toString());
        assertEquals("someUser", key.getColumnFamily().toString());
        assertEquals("", key.getColumnQualifier().toString());
        assertEquals("ALL", key.getColumnVisibility().toString());
        assertEquals(ap.toString(), value.toString());
    }
    
}
