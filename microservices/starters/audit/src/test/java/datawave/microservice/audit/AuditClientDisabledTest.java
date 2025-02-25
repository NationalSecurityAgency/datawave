package datawave.microservice.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.microservice.audit.config.AuditServiceConfiguration;
import datawave.microservice.audit.replay.ReplayClient;

/**
 * Tests to make sure that bean injection for {@link AuditClient} can be disabled via config {@code audit.enabled=false})
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest
@ActiveProfiles({"audit-disabled"})
public class AuditClientDisabledTest {
    
    @Autowired
    ApplicationContext context;
    
    @Test
    public void verifyAutoConfig() {
        assertEquals(0, context.getBeanNamesForType(AuditClient.class).length, "No AuditClient beans should have been found");
        assertEquals(0, context.getBeanNamesForType(ReplayClient.class).length, "No ReplayClient beans should have been found");
        assertEquals(0, context.getBeanNamesForType(AuditServiceConfiguration.class).length, "No AuditServiceConfiguration beans should have been found");
        assertEquals(0, context.getBeanNamesForType(AuditServiceProvider.class).length, "No AuditServiceProvider beans should have been found");
    }
    
    @SpringBootApplication(scanBasePackages = "datawave.microservice")
    public static class TestApplication {
        public static void main(String[] args) {
            SpringApplication.run(TestApplication.class, args);
        }
    }
}
