package datawave.microservice.accumulo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertFalse;
import static org.springframework.test.util.AssertionErrors.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "spring.main.allow-bean-definition-overriding=true")
@ComponentScan(basePackages = "datawave.microservice")
@ActiveProfiles({"all-services-disabled"})
public class AllServicesDisabledTest {
    
    @Autowired
    private ApplicationContext context;
    
    @Test
    public void verifyAutoConfig() {
        assertTrue("accumuloService bean should have been found", context.containsBean("accumuloService"));
        assertFalse("auditServiceConfiguration bean should not have been found", context.containsBean("auditServiceConfiguration"));
        assertFalse("auditServiceInstanceProvider bean should not have been found", context.containsBean("auditServiceInstanceProvider"));
        assertFalse("auditLookupSecurityMarking bean should not have been found", context.containsBean("auditLookupSecurityMarking"));
        assertFalse("lookupService bean should not have been found", context.containsBean("lookupService"));
        assertFalse("lookupController bean should not have been found", context.containsBean("lookupController"));
        assertFalse("statsController bean should not have been found", context.containsBean("statsController"));
        assertFalse("statsService bean should not have been found", context.containsBean("statsService"));
        assertFalse("adminController bean should not have been found", context.containsBean("adminController"));
        assertFalse("adminService bean should not have been found", context.containsBean("adminService"));
    }
}
