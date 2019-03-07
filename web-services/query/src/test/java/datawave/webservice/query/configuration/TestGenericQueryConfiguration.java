package datawave.webservice.query.configuration;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;

import datawave.webservice.query.logic.BaseQueryLogic;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class TestGenericQueryConfiguration {
    
    @Mock
    Authorizations authorizations;
    
    @Mock
    BaseQueryLogic<?> baseQueryLogic;
    
    @Mock
    Connector connector;
    
    @Test
    public void testConstructor_WithConfiguredLogic() throws Exception {
        // Set expectations
        expect(this.baseQueryLogic.getTableName()).andReturn("TEST");
        expect(this.baseQueryLogic.getBaseIteratorPriority()).andReturn(100);
        expect(this.baseQueryLogic.getMaxRowsToScan()).andReturn(1000L);
        expect(this.baseQueryLogic.getUndisplayedVisibilities()).andReturn(new HashSet<>(0));
        
        // Run the test
        PowerMock.replayAll();
        GenericQueryConfiguration subject = new GenericQueryConfiguration(this.baseQueryLogic) {};
        boolean result1 = subject.canRunQuery();
        PowerMock.verifyAll();
        
        // Verify results
        assertFalse("Query should not be runnable", result1);
    }
    
    @Test
    public void testCanRunQuery_HappyPath() throws Exception {
        // Run the test
        PowerMock.replayAll();
        GenericQueryConfiguration subject = new GenericQueryConfiguration() {};
        subject.setConnector(this.connector);
        subject.setAuthorizations(new HashSet<>(Arrays.asList(this.authorizations)));
        subject.setBeginDate(new Date());
        subject.setEndDate(new Date());
        boolean result1 = subject.canRunQuery();
        PowerMock.verifyAll();
        
        // Verify results
        assertTrue("Query should be runnable", result1);
    }
}
