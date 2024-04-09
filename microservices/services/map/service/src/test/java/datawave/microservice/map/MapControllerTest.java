package datawave.microservice.map;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.client.DefaultResponseErrorHandler;

import datawave.microservice.authorization.jwt.JWTRestTemplate;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = {MapControllerTest.MapControllerTestConfiguration.class, MapService.class})
public class MapControllerTest {
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    private JWTRestTemplate jwtRestTemplate;
    
    @BeforeEach
    public void setup() {
        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        jwtRestTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
            @Override
            public void handleError(ClientHttpResponse response) throws IOException {
                // do nothing
            }
        });
    }
    
    // @Test
    // public void someTest() {
    //
    // }
    
    @Configuration
    public static class MapControllerTestConfiguration {
        // configuration overrides go here
    }
}
