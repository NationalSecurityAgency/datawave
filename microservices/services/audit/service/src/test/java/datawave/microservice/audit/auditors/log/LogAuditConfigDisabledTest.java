package datawave.microservice.audit.auditors.log;

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
@ContextConfiguration(classes = LogAuditConfigDisabledTest.LogAuditConfigTestConfiguration.class)
@ActiveProfiles({"LogAuditConfigDisabledTest", "log-disabled"})
public class LogAuditConfigDisabledTest {
    
    @Autowired
    private ApplicationContext context;
    
    @Test
    public void testBeansMissing() {
        assertFalse(context.containsBean("logAuditMessageHandler"));
        assertFalse(context.containsBean("logAuditor"));
    }
    
    @Configuration
    @Profile("LogAuditConfigDisabledTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class LogAuditConfigTestConfiguration {
        
    }
}
