package datawave.microservice.audit.auditors.accumulo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.audit.auditors.accumulo.config.AccumuloAuditProperties;
import datawave.microservice.audit.auditors.accumulo.config.AccumuloAuditProperties.Accumulo;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = "spring.main.allow-bean-definition-overriding=true")
@ActiveProfiles({"AccumuloAuditorTest", "accumulo-enabled"})
public class AccumuloAuditorTest {
    
    @Autowired
    private Auditor accumuloAuditor;
    
    @Autowired
    private AccumuloAuditProperties accumuloAuditProperties;
    
    @Autowired
    private AccumuloClient accumuloClient;
    
    @Autowired
    private ApplicationContext context;
    
    @Test
    public void testBeansPresent() {
        assertTrue(context.containsBean("accumuloAuditSink"));
        assertTrue(context.containsBean("accumuloAuditor"));
    }
    
    @Test
    public void testInit() throws Exception {
        String tableName = "QueryAuditTable";
        if (accumuloClient.tableOperations().exists(tableName))
            accumuloClient.tableOperations().delete(tableName);
        
        assertFalse(accumuloClient.tableOperations().exists(tableName), tableName + " already exists before test");
        
        Auditor accumuloAuditor = new AccumuloAuditor(tableName, accumuloClient);
        
        assertTrue(accumuloClient.tableOperations().exists(tableName), tableName + " doesn't exist after test");
        
        accumuloAuditor = new AccumuloAuditor(tableName, accumuloClient);
        
        assertTrue(accumuloClient.tableOperations().exists(tableName), tableName + " doesn't exist after test");
    }
    
    @Test
    public void testActiveAudit() throws Exception {
        accumuloClient.tableOperations().deleteRows(accumuloAuditProperties.getTableName(), null, null);
        
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
        
        Scanner scanner = accumuloClient.createScanner(accumuloAuditProperties.getTableName(), new Authorizations("ALL"));
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
        accumuloClient.tableOperations().deleteRows(accumuloAuditProperties.getTableName(), null, null);
        
        Date date = new Date();
        
        AuditParameters auditParams = new AuditParameters();
        auditParams.setUserDn("someUser");
        auditParams.setAuths("AUTH1,AUTH2");
        auditParams.setQuery("test query");
        auditParams.setAuditType(Auditor.AuditType.NONE);
        auditParams.setColviz(new ColumnVisibility("ALL"));
        auditParams.setQueryDate(date);
        
        accumuloAuditor.audit(auditParams);
        
        Scanner scanner = accumuloClient.createScanner(accumuloAuditProperties.getTableName(), new Authorizations("ALL"));
        Iterator<Map.Entry<Key,Value>> it = scanner.iterator();
        assertFalse(it.hasNext());
    }
    
    @Test
    public void testMissingUserDN() {
        Date date = new Date();
        
        AuditParameters auditParams = new AuditParameters();
        auditParams.setAuths("AUTH1,AUTH2");
        auditParams.setQuery("test query");
        auditParams.setAuditType(Auditor.AuditType.ACTIVE);
        auditParams.setColviz(new ColumnVisibility("ALL"));
        auditParams.setQueryDate(date);
        
        assertThrows(NullPointerException.class, () -> accumuloAuditor.audit(auditParams));
    }
    
    @Test
    public void testMissingColViz() {
        Date date = new Date();
        
        AuditParameters auditParams = new AuditParameters();
        auditParams.setUserDn("someUser");
        auditParams.setAuths("AUTH1,AUTH2");
        auditParams.setQuery("test query");
        auditParams.setAuditType(Auditor.AuditType.ACTIVE);
        auditParams.setQueryDate(date);
        
        assertThrows(NullPointerException.class, () -> accumuloAuditor.audit(auditParams));
    }
    
    @Configuration
    @Profile("AccumuloAuditorTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class AccumuloAuditorTestConfiguration {
        @Bean
        public AccumuloClient accumuloClient(AccumuloAuditProperties accumuloAuditProperties) throws AccumuloSecurityException {
            Accumulo accumulo = accumuloAuditProperties.getAccumuloConfig();
            return new InMemoryAccumuloClient(accumulo.getUsername(), new InMemoryInstance(accumulo.getInstanceName()));
        }
    }
}
