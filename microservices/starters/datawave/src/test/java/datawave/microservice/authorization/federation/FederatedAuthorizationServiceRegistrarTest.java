package datawave.microservice.authorization.federation;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"federatedAuthorizationServiceTest"})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class FederatedAuthorizationServiceRegistrarTest {
    
    @Autowired
    private ApplicationContext applicationContext;
    
    @Test
    public void federatedAuthorizationServiceTest() {
        FederatedAuthorizationService federatedAuthorizationService = (FederatedAuthorizationService) applicationContext
                        .getBean("FederatedAuthorizationService");
        
        assertNotNull(federatedAuthorizationService);
    }
    
    @SpringBootApplication(scanBasePackages = "datawave.microservice")
    public static class TestConfiguration {}
}
