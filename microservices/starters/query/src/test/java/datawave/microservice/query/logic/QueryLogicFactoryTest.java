package datawave.microservice.query.logic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.query.logic.config.QueryLogicFactoryProperties;
import datawave.security.authorization.JWTTokenHandler;
import datawave.webservice.query.exception.QueryException;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@ActiveProfiles({"QueryStarterDefaults", "QueryLogicFactoryTest"})
public class QueryLogicFactoryTest {
    
    @Autowired
    private QueryLogicFactoryProperties queryLogicFactoryProperties;
    
    @Autowired
    private QueryLogicFactory queryLogicFactory;
    
    @Autowired
    private JWTTokenHandler jwtTokenHandler;
    
    @Test
    public void createShardQueryLogicTest() throws QueryException, CloneNotSupportedException {
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("FederatedEventQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("ContentQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("CountQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("DiscoveryQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("EdgeEventQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("ErrorCountQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("ErrorDiscoveryQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("ErrorEventQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("ErrorFieldIndexCountQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("EventQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("FacetedQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("FieldIndexCountQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("HitHighlights"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("IndexStatsQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("InternalQueryMetricsQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("LuceneUUIDEventQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("QueryMetricsQuery"));
        Assertions.assertNotNull(queryLogicFactory.getQueryLogic("TermFrequencyQuery"));
    }
    
    @ComponentScan(basePackages = "datawave.microservice")
    @Configuration
    @Profile("QueryLogicFactoryTest")
    public static class TestConfiguration {
        
    }
    
    @SpringBootApplication(scanBasePackages = "datawave.microservice", exclude = {ErrorMvcAutoConfiguration.class})
    public static class TestApplication {
        public static void main(String[] args) {
            SpringApplication.run(QueryLogicFactoryTest.TestApplication.class, args);
        }
    }
}
