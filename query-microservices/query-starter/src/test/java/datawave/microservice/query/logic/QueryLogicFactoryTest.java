package datawave.microservice.query.logic;

import datawave.services.query.logic.QueryLogic;
import datawave.services.query.logic.QueryLogicFactory;
import datawave.webservice.query.exception.QueryException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"QueryStarterDefaults", "QueryLogicFactoryTest"})
public class QueryLogicFactoryTest {
    
    @Autowired
    QueryLogicFactory queryLogicFactory;
    
    @Test
    public void createShardQueryLogicTest() throws QueryException, CloneNotSupportedException {
        QueryLogic<?> queryLogic = queryLogicFactory.getQueryLogic("EventQuery");
        
        System.out.println("done!");
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
