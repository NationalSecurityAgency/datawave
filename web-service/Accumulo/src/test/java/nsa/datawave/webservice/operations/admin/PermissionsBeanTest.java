package nsa.datawave.webservice.operations.admin;

import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import nsa.datawave.webservice.result.VoidResponse;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

public class PermissionsBeanTest {
    
    private PermissionsBean permissionsBean;
    private AccumuloConnectionFactory connectionFactory;
    private Connector connector;
    private SecurityOperations securityOperations;
    
    @Before
    public void setUp() throws Exception {
        permissionsBean = new PermissionsBean();
        connectionFactory = mock(AccumuloConnectionFactory.class);
        connector = mock(Connector.class);
        securityOperations = mock(SecurityOperations.class);
        expect(connector.securityOperations()).andReturn(securityOperations);
        replay(connector);
        
        Field field = permissionsBean.getClass().getDeclaredField("connectionFactory");
        field.setAccessible(true);
        field.set(permissionsBean, connectionFactory);
        
        Map<String,String> trackingMap = new HashMap<String,String>();
        expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getConnection(AccumuloConnectionFactory.Priority.ADMIN, trackingMap)).andReturn(connector);
        connectionFactory.returnConnection(connector);
        replay(connectionFactory);
    }
    
    @Test
    public void testGrantSystemPermission() throws Exception {
        VoidResponse voidResponse = permissionsBean.grantSystemPermission("test", "GRANT");
        assertNotNull(voidResponse);
    }
    
    @Test
    public void testGrantTablePermission() throws Exception {
        VoidResponse voidResponse = permissionsBean.grantTablePermission("test", "table", "GRANT");
        assertNotNull(voidResponse);
    }
    
    @Test
    public void testRevokeSystemPermission() throws Exception {
        VoidResponse voidResponse = permissionsBean.revokeSystemPermission("test", "GRANT");
        assertNotNull(voidResponse);
    }
    
    @Test
    public void testRevokeTablePermission() throws Exception {
        VoidResponse voidResponse = permissionsBean.revokeTablePermission("test", "table", "GRANT");
        assertNotNull(voidResponse);
    }
}
