package datawave.webservice.query.configuration;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.core.query.logic.BaseQueryLogic;

@RunWith(PowerMockRunner.class)
public class GenericQueryConfigurationMockTest {

    @Mock
    Authorizations authorizations;

    @Mock
    BaseQueryLogic<?> baseQueryLogic;

    @Mock
    AccumuloClient client;

    @Mock
    GenericQueryConfiguration config;

    @Before
    public void setup() {
        this.config = new GenericQueryConfiguration() {
            @Override
            public Iterator<QueryData> getQueriesIter() {
                return super.getQueriesIter();
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
        PowerMock.replayAll();
        GenericQueryConfiguration subject = new GenericQueryConfiguration(this.baseQueryLogic) {};
        boolean result1 = subject.canRunQuery();
        PowerMock.verifyAll();

        // Verify results
        assertFalse("Query should not be runnable", result1);
    }

    @Test
    public void testCanRunQuery_HappyPath() {
        expect(this.authorizations.getAuthorizations()).andReturn(Collections.emptyList());

        // Run the test
        PowerMock.replayAll();
        GenericQueryConfiguration subject = new GenericQueryConfiguration() {};
        subject.setClient(this.client);
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
        assertEquals(-1L, config.getMaxWork().longValue());
        assertEquals(100, config.getBaseIteratorPriority());
        assertFalse(config.getBypassAccumulo());
    }
}
