package datawave.webservice.operations.admin;

import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.result.VoidResponse;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

public class TableAdminBeanTest {
    
    private TableAdminBean tableAdminBean;
    private AccumuloConnectionFactory connectionFactory;
    private Connector connector;
    private TableOperations tableOperations;
    
    @Before
    public void setUp() throws Exception {
        tableAdminBean = new TableAdminBean();
        connectionFactory = mock(AccumuloConnectionFactory.class);
        connector = mock(Connector.class);
        tableOperations = mock(TableOperations.class);
        expect(connector.tableOperations()).andReturn(tableOperations);
        replay(connector);
        
        Field field = tableAdminBean.getClass().getDeclaredField("connectionFactory");
        field.setAccessible(true);
        field.set(tableAdminBean, connectionFactory);
    }
    
    private void setConnectionFactory(AccumuloConnectionFactory.Priority priority) throws Exception {
        Map<String,String> trackingMap = new HashMap<String,String>();
        expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getConnection(priority, trackingMap)).andReturn(connector);
        connectionFactory.returnConnection(connector);
        replay(connectionFactory);
    }
    
    @Test
    public void testCreateTableExists() throws Exception {
        setConnectionFactory(AccumuloConnectionFactory.Priority.NORMAL);
        VoidResponse voidResponse = tableAdminBean.createTable("table");
        assertNotNull(voidResponse);
    }
    
    @Test
    public void testFlushTable() throws Exception {
        setConnectionFactory(AccumuloConnectionFactory.Priority.ADMIN);
        VoidResponse voidResponse = tableAdminBean.flushTable("table");
        assertNotNull(voidResponse);
    }
    
    @Test
    public void testSetTableProperty() throws Exception {
        setConnectionFactory(AccumuloConnectionFactory.Priority.ADMIN);
        VoidResponse voidResponse = tableAdminBean.setTableProperty("table", "property", "value");
        assertNotNull(voidResponse);
    }
    
    @Test
    public void testRemoveTableProperty() throws Exception {
        setConnectionFactory(AccumuloConnectionFactory.Priority.ADMIN);
        VoidResponse voidResponse = tableAdminBean.removeTableProperty("table", "property");
        assertNotNull(voidResponse);
    }
}
