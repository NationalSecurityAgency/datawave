package datawave.microservice.query.logic;

import datawave.webservice.query.exception.QueryException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ContextConfiguration(classes = QueryLogicFactoryTest.TestConfiguration.class)
@ActiveProfiles({"QueryLogicFactoryTest"})
public class QueryLogicFactoryTest {
    
    @Autowired
    QueryLogicFactory queryLogicFactory;
    
    @Test
    public void queryLogicFactoryInitTest() throws QueryException, CloneNotSupportedException {
        QueryLogic<?> queryLogic = queryLogicFactory.getQueryLogic("EventQuery");
        
        System.out.println("done!");
    }
    
    @Configuration
    @Profile("QueryLogicFactoryTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class TestConfiguration {
        
    }
    
    @SpringBootApplication(scanBasePackages = "datawave.microservice")
    public static class TestApplication {
        public static void main(String[] args) {
            SpringApplication.run(QueryLogicFactoryTest.TestApplication.class, args);
        }
    }
}
