package datawave.microservice.audit.accumulo;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.audit.accumulo.config.AccumuloAuditProperties;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.context.annotation.RequestScope;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = AccumuloAuditorTest.AccumuloAuditorTestConfiguration.class)
@ActiveProfiles({"AccumuloAuditorTest", "accumulo-enabled"})
public class AccumuloAuditorTest {
    
    @Autowired
    private Auditor accumuloAuditor;
    
    @Autowired
    private AccumuloAuditProperties accumuloAuditProperties;
    
    @Autowired
    private Connector connector;
    
    @Autowired
    private ApplicationContext context;
    
    @Test
    public void testBeansPresent() {
        assertTrue(context.containsBean("accumuloAuditMessageHandler"));
        assertTrue(context.containsBean("accumuloAuditor"));
    }
    
    @Test
    public void testInit() throws Exception {
        String tableName = "QueryAuditTable";
        if (connector.tableOperations().exists(tableName))
            connector.tableOperations().delete(tableName);
        
        assertFalse(tableName + " already exists before test", connector.tableOperations().exists(tableName));
        
        Auditor accumuloAuditor = new AccumuloAuditor(tableName, connector);
        
        assertTrue(tableName + " doesn't exist after test", connector.tableOperations().exists(tableName));
        
        accumuloAuditor = new AccumuloAuditor(tableName, connector);
        
        assertTrue(tableName + " doesn't exist after test", connector.tableOperations().exists(tableName));
    }
    
    @Test
    public void testActiveAudit() throws Exception {
        connector.tableOperations().deleteRows(accumuloAuditProperties.getTableName(), null, null);
        
        SimpleDateFormat formatter = new SimpleDateFormat(Auditor.ISO_8601_FORMAT_STRING);
        
        Date date = new Date();
        
        AuditParameters auditParams = new AuditParameters();
        auditParams.setUserDn("someUser");
        auditParams.setAuths("AUTH1,AUTH2");
        auditParams.setQuery("test query");
        auditParams.setAuditType(Auditor.AuditType.ACTIVE);
        auditParams.setColviz(new ColumnVisibility("ALL"));
        auditParams.setQueryDate(date);
        
        accumuloAuditor.audit(auditParams);
        
        Scanner scanner = connector.createScanner(accumuloAuditProperties.getTableName(), new Authorizations("ALL"));
        Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
        assertTrue(it.hasNext());
        Map.Entry<Key,Value> entry = it.next();
        Key key = entry.getKey();
        Value value = entry.getValue();
        assertEquals(formatter.format(date), key.getRow().toString());
        assertEquals("someUser", key.getColumnFamily().toString());
        assertEquals("", key.getColumnQualifier().toString());
        assertEquals("ALL", key.getColumnVisibility().toString());
        assertEquals(auditParams.toString(), value.toString());
    }
    
    @Test
    public void testNoneAudit() throws Exception {
        connector.tableOperations().deleteRows(accumuloAuditProperties.getTableName(), null, null);
        
        Date date = new Date();
        
        AuditParameters auditParams = new AuditParameters();
        auditParams.setUserDn("someUser");
        auditParams.setAuths("AUTH1,AUTH2");
        auditParams.setQuery("test query");
        auditParams.setAuditType(Auditor.AuditType.NONE);
        auditParams.setColviz(new ColumnVisibility("ALL"));
        auditParams.setQueryDate(date);
        
        accumuloAuditor.audit(auditParams);
        
        Scanner scanner = connector.createScanner(accumuloAuditProperties.getTableName(), new Authorizations("ALL"));
        Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
        assertFalse(it.hasNext());
    }
    
    @Test(expected = NullPointerException.class)
    public void testMissingUserDN() throws Exception {
        Date date = new Date();
        
        AuditParameters auditParams = new AuditParameters();
        auditParams.setAuths("AUTH1,AUTH2");
        auditParams.setQuery("test query");
        auditParams.setAuditType(Auditor.AuditType.ACTIVE);
        auditParams.setColviz(new ColumnVisibility("ALL"));
        auditParams.setQueryDate(date);
        
        accumuloAuditor.audit(auditParams);
    }
    
    @Test(expected = NullPointerException.class)
    public void testMissingColViz() throws Exception {
        Date date = new Date();
        
        AuditParameters auditParams = new AuditParameters();
        auditParams.setUserDn("someUser");
        auditParams.setAuths("AUTH1,AUTH2");
        auditParams.setQuery("test query");
        auditParams.setAuditType(Auditor.AuditType.ACTIVE);
        auditParams.setQueryDate(date);
        
        accumuloAuditor.audit(auditParams);
    }
    
    @Configuration
    @Profile("AccumuloAuditorTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class AccumuloAuditorTestConfiguration {
        @Bean("restAuditParams")
        @RequestScope
        public AuditParameters restAuditParams() {
            return new AuditParameters();
        }
        
        @Bean("msgHandlerAuditParams")
        public AuditParameters msgHandlerAuditParams() {
            return new AuditParameters();
        }
        
        @Bean
        public Instance accumuloInstance(AccumuloAuditProperties accumuloAuditProperties) {
            return new InMemoryInstance(accumuloAuditProperties.getAccumuloConfig().getInstanceName());
        }
        
        @Bean
        public Connector connector(AccumuloAuditProperties accumuloAuditProperties, Instance accumuloInstance)
                        throws AccumuloSecurityException, AccumuloException {
            return accumuloInstance.getConnector(accumuloAuditProperties.getAccumuloConfig().getUsername(),
                            new PasswordToken(accumuloAuditProperties.getAccumuloConfig().getPassword()));
        }
    }
}
