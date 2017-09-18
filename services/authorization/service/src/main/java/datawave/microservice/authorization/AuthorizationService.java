package datawave.microservice.authorization;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.ErrorMvcAutoConfiguration;

/**
 * Launcher for the authorization service.
 */
@SpringBootApplication(scanBasePackages = "datawave.microservice")
@EnableAutoConfiguration(exclude = {ErrorMvcAutoConfiguration.class})
public class AuthorizationService {
    public static void main(String[] args) {
        SpringApplication.run(AuthorizationService.class, args);
    }
}
