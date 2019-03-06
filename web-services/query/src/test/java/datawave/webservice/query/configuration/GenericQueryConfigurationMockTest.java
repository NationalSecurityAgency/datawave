package datawave.webservice.query.configuration;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

import datawave.webservice.query.logic.BaseQueryLogic;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class GenericQueryConfigurationMockTest {
    
    @Mock
    Authorizations authorizations;
    
    @Mock
    BaseQueryLogic<?> baseQueryLogic;
    
    @Mock
    Connector connector;
    
    @Mock
    GenericQueryConfiguration config;
    
    @Before
    public void setup() {
        this.config = new GenericQueryConfiguration() {
            @Override
            public Iterator<QueryData> getQueries() {
                return super.getQueries();
            }
        };
    }
    
    @Test
    public void testConstructor_WithConfiguredLogic() {
        // Set expectations
        expect(this.baseQueryLogic.getTableName()).andReturn("TEST");
        expect(this.baseQueryLogic.getBaseIteratorPriority()).andReturn(100);
        expect(this.baseQueryLogic.getMaxRowsToScan()).andReturn(1000L);
        expect(this.baseQueryLogic.getUndisplayedVisibilities()).andReturn(new HashSet<>(0));
        expect(this.baseQueryLogic.getBypassAccumulo()).andReturn(false);
        
        // Run the test
        PowerMock.replayAll();
        GenericQueryConfiguration subject = new GenericQueryConfiguration(this.baseQueryLogic) {};
        boolean result1 = subject.canRunQuery();
        PowerMock.verifyAll();
        
        // Verify results
        assertFalse("Query should not be runnable", result1);
    }
    
    @Test
    public void testCanRunQuery_HappyPath() {
        // Run the test
        PowerMock.replayAll();
        GenericQueryConfiguration subject = new GenericQueryConfiguration() {};
        subject.setConnector(this.connector);
        subject.setAuthorizations(new HashSet<>(Collections.singletonList(this.authorizations)));
        subject.setBeginDate(new Date());
        subject.setEndDate(new Date());
        boolean result1 = subject.canRunQuery();
        PowerMock.verifyAll();
        
        // Verify results
        assertTrue("Query should be runnable", result1);
    }
    
    @Test
    public void testBasicInit() {
        // Assert good init
        assertEquals("shard", config.getTableName());
        assertEquals(25000L, config.getMaxRowsToScan().longValue());
        assertEquals(new HashSet<>(), config.getUndisplayedVisibilities());
        assertEquals(100, config.getBaseIteratorPriority());
        assertFalse(config.getBypassAccumulo());
    }
}
