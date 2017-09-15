package datawave.microservice.config.web;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Configuration to generate Swagger documentation for REST endpoints.
 */
@Configuration
@EnableSwagger2
public class SwaggerConfig {
    @Bean
    public Docket api(@Value("${spring.application.name}") String appName) {
        // @formatter:off
        return new Docket(DocumentationType.SWAGGER_2)
            .select()
            .apis(RequestHandlerSelectors.basePackage("datawave.microservice"))
            .paths(PathSelectors.any())
            .build()
            .apiInfo(apiInfo(appName, SwaggerConfig.class.getPackage().getImplementationVersion()));
        // @formatter:on
    }
    
    private ApiInfo apiInfo(String appName, String appVersion) {
        return new ApiInfo(appName + " API", "REST operations provided by the " + appName + " API", appVersion, null, (Contact) null, null, null);
    }
}
