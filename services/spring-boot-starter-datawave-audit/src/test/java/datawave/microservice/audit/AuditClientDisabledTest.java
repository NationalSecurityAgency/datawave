package datawave.microservice.audit;

import datawave.microservice.audit.config.AuditServiceConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

/**
 * Tests to make sure that bean injection for {@link AuditClient} can be disabled via config {@code audit.enabled=false})
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"audit-disabled"})
public class AuditClientDisabledTest {
    
    @Autowired
    ApplicationContext context;
    
    @Test
    public void verifyAutoConfig() {
        assertEquals("No AuditClient beans should have been found", 0, context.getBeanNamesForType(AuditClient.class).length);
        assertEquals("No AuditServiceConfiguration beans should have been found", 0, context.getBeanNamesForType(AuditServiceConfiguration.class).length);
        assertEquals("No AuditServiceProvider beans should have been found", 0, context.getBeanNamesForType(AuditServiceProvider.class).length);
    }
    
    @SpringBootApplication(scanBasePackages = "datawave.microservice")
    public static class TestApplication {
        public static void main(String[] args) {
            SpringApplication.run(TestApplication.class, args);
        }
    }
}
