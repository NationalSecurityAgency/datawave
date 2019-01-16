package datawave.webservice.operations.admin;

import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.request.UpdateRequest;
import datawave.webservice.response.UpdateResponse;
import datawave.webservice.response.ValidateVisibilityResponse;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;

public class UpdateBeanTest {
    
    private UpdateBean updateBean;
    private Connector connector;
    
    @Before
    public void setUp() throws Exception {
        updateBean = new UpdateBean();
        updateBean.init();
        AccumuloConnectionFactory connectionFactory = mock(AccumuloConnectionFactory.class);
        connector = mock(Connector.class);
        
        Field field = updateBean.getClass().getDeclaredField("connectionFactory");
        field.setAccessible(true);
        field.set(updateBean, connectionFactory);
        
        Map<String,String> trackingMap = new HashMap<>();
        expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(trackingMap);
        expect(connectionFactory.getConnection(AccumuloConnectionFactory.Priority.ADMIN, trackingMap)).andReturn(connector);
        connectionFactory.returnConnection(connector);
        replay(connectionFactory);
    }
    
    @Test
    public void testDoUpdate() throws Exception {
        MultiTableBatchWriter multiTableBatchWriter = mock(MultiTableBatchWriter.class);
        expect(connector.createMultiTableBatchWriter(new BatchWriterConfig().setMaxLatency(3, TimeUnit.SECONDS).setMaxMemory(50000).setMaxWriteThreads(5)))
                        .andReturn(multiTableBatchWriter);
        replay(connector);
        UpdateRequest updateRequest = new UpdateRequest();
        UpdateResponse updateResponse = updateBean.doUpdate(updateRequest);
    }
    
    @Test
    public void testValidateVisibilities() throws Exception {
        SecurityOperations securityOperations = mock(SecurityOperations.class);
        expect(connector.securityOperations()).andReturn(securityOperations);
        expect(connector.whoami()).andReturn("user");
        replay(connector);
        String[] visibilities = new String[] {};
        ValidateVisibilityResponse updateResponse = updateBean.validateVisibilities(visibilities);
    }
}
