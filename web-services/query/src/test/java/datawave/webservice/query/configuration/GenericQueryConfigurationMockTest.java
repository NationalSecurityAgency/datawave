package datawave.webservice.query.configuration;

import datawave.webservice.query.logic.BaseQueryLogic;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(EasyMockExtension.class)
public class GenericQueryConfigurationMockTest extends EasyMockSupport {
    
    @Mock
    Authorizations authorizations;
    
    @Mock
    BaseQueryLogic<?> baseQueryLogic;
    
    @Mock
    Connector connector;
    
    @Mock
    GenericQueryConfiguration config;
    
    @BeforeEach
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
        GenericQueryConfiguration oldConfig = new GenericQueryConfiguration() {};
        oldConfig.setTableName("TEST");
        oldConfig.setBaseIteratorPriority(100);
        oldConfig.setMaxWork(1000L);
        oldConfig.setBypassAccumulo(false);
        
        expect(this.baseQueryLogic.getConfig()).andReturn(oldConfig).anyTimes();
        
        // Run the test
        replayAll();
        GenericQueryConfiguration subject = new GenericQueryConfiguration(this.baseQueryLogic) {};
        boolean result1 = subject.canRunQuery();
        verifyAll();
        
        // Verify results
        assertFalse(result1, "Query should not be runnable");
    }
    
    @Test
    public void testCanRunQuery_HappyPath() {
        // Run the test
        GenericQueryConfiguration subject = new GenericQueryConfiguration() {};
        subject.setConnector(this.connector);
        subject.setAuthorizations(new HashSet<>(Collections.singletonList(this.authorizations)));
        subject.setBeginDate(new Date());
        subject.setEndDate(new Date());
        boolean result1 = subject.canRunQuery();
        
        // Verify results
        assertTrue(result1, "Query should be runnable");
    }
    
    @Test
    public void testBasicInit() {
        // Assert good init
        assertEquals("shard", config.getTableName());
        assertEquals(-1L, config.getMaxWork().longValue());
        assertEquals(100, config.getBaseIteratorPriority());
        assertFalse(config.getBypassAccumulo());
    }
}
