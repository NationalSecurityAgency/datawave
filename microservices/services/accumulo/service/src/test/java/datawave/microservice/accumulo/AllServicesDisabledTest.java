package datawave.microservice.accumulo;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "spring.main.allow-bean-definition-overriding=true")
@ComponentScan(basePackages = "datawave.microservice")
@ActiveProfiles({"all-services-disabled"})
public class AllServicesDisabledTest {
    
    @Autowired
    private ApplicationContext context;
    
    @Test
    public void verifyAutoConfig() {
        assertTrue(context.containsBean("accumuloService"), "accumuloService bean should have been found");
        assertFalse(context.containsBean("auditServiceConfiguration"), "auditServiceConfiguration bean should not have been found");
        assertFalse(context.containsBean("auditServiceInstanceProvider"), "auditServiceInstanceProvider bean should not have been found");
        assertFalse(context.containsBean("auditLookupSecurityMarking"), "auditLookupSecurityMarking bean should not have been found");
        assertFalse(context.containsBean("lookupService"), "lookupService bean should not have been found");
        assertFalse(context.containsBean("lookupController"), "lookupController bean should not have been found");
        assertFalse(context.containsBean("statsController"), "statsController bean should not have been found");
        assertFalse(context.containsBean("statsService"), "statsService bean should not have been found");
        assertFalse(context.containsBean("adminController"), "adminController bean should not have been found");
        assertFalse(context.containsBean("adminService"), "adminService bean should not have been found");
    }
}
