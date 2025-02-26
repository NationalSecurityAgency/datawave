package datawave.microservice.audit.auditors.accumulo;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = AccumuloAuditConfigMissingTest.AccumuloAuditConfigTestConfiguration.class)
@ActiveProfiles({"AccumuloAuditConfigMissingTest", "missing"})
public class AccumuloAuditConfigMissingTest {
    
    @Autowired
    private ApplicationContext context;
    
    @Test
    public void testBeansMissing() {
        assertFalse(context.containsBean("accumuloAuditMessageHandler"));
        assertFalse(context.containsBean("accumuloAuditor"));
        assertFalse(context.containsBean("connector"));
    }
    
    @Configuration
    @Profile("AccumuloAuditConfigMissingTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class AccumuloAuditConfigTestConfiguration {
        
    }
}
