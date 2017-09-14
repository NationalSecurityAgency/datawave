package datawave.microservice.authorization.jwt;

import org.springframework.boot.web.client.RestTemplateCustomizer;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class JWTRestTemplateCustomizer implements RestTemplateCustomizer {
    private final JWTTokenHandler jwtTokenHandler;
    
    public JWTRestTemplateCustomizer(JWTTokenHandler jwtTokenHandler) {
        this.jwtTokenHandler = jwtTokenHandler;
    }
    
    @Override
    public void customize(RestTemplate restTemplate) {
        if (restTemplate instanceof JWTRestTemplate) {
            ((JWTRestTemplate) restTemplate).setJwtTokenHandler(jwtTokenHandler);
        }
    }
}
