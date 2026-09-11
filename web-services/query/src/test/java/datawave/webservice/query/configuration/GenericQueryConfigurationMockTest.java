package datawave.webservice.query.configuration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.Test;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;

public class GenericQueryConfigurationMockTest {

    private final Authorizations authorizations = mock(Authorizations.class);
    private final BaseQueryLogic<?> baseQueryLogic = mock(BaseQueryLogic.class);
    private final AccumuloClient client = mock(AccumuloClient.class);

    @Test
    public void testConstructor_WithConfiguredLogic() {
        GenericQueryConfiguration oldConfig = new GenericQueryConfiguration() {};
        oldConfig.setTableName("TEST");
        oldConfig.setBaseIteratorPriority(100);
        oldConfig.setMaxWork(1000L);
        oldConfig.setBypassAccumulo(false);

        when(baseQueryLogic.getConfig()).thenReturn(oldConfig);

        GenericQueryConfiguration subject = new GenericQueryConfiguration(this.baseQueryLogic) {};
        assertFalse(subject.canRunQuery(), "Query should not be runnable");
    }

    @Test
    public void testCanRunQuery_HappyPath() {
        when(authorizations.getAuthorizations()).thenReturn(Collections.emptyList());

        GenericQueryConfiguration subject = new GenericQueryConfiguration() {};
        subject.setClient(this.client);
        subject.setAuthorizations(new HashSet<>(Collections.singletonList(this.authorizations)));
        subject.setBeginDate(new Date());
        subject.setEndDate(new Date());

        assertTrue(subject.canRunQuery(), "Query should be runnable");
    }
}
