package datawave.microservice.annotation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

import lombok.extern.slf4j.Slf4j;

/**
 * Launcher for the annotation service
 */
@EnableDiscoveryClient
@SpringBootApplication(scanBasePackages = {"datawave.microservice"}, exclude = {ErrorMvcAutoConfiguration.class})
@Slf4j
public class AnnotationService {
    public static void main(String[] args) {

        log.info("Launching AnnotationService");
        for (String arg : args) {
            log.info("arg: {}", arg);
        }

        SpringApplication.run(AnnotationService.class, args);
    }
}
