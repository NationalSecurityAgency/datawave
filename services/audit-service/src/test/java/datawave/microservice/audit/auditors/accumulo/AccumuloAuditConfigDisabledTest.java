package datawave.microservice.audit.auditors.accumulo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertFalse;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = AccumuloAuditConfigDisabledTest.AccumuloAuditConfigTestConfiguration.class)
@ActiveProfiles({"AccumuloAuditConfigDisabledTest", "accumulo-disabled"})
public class AccumuloAuditConfigDisabledTest {
    
    @Autowired
    private ApplicationContext context;
    
    @Test
    public void testBeansMissing() {
        assertFalse(context.containsBean("accumuloAuditMessageHandler"));
        assertFalse(context.containsBean("accumuloAuditor"));
        assertFalse(context.containsBean("connector"));
    }
    
    @Configuration
    @Profile("AccumuloAuditConfigDisabledTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class AccumuloAuditConfigTestConfiguration {
        
    }
}
