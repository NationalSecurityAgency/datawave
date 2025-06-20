package datawave.microservice.query.remote;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"remoteQueryServiceTest", "federation", "QueryStarterDefaults"})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class MSRemoteQueryServiceRegistrarTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    public void federatedAuthorizationServiceTest() {
        MSRemoteQueryService msRemoteQueryService = (MSRemoteQueryService) applicationContext.getBean("RemoteQueryService");

        assertNotNull(msRemoteQueryService);
    }

    @SpringBootApplication(scanBasePackages = "datawave.microservice")
    public static class TestConfiguration {}
}
