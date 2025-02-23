package datawave.microservice.config.web;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;

/**
 * Configuration to generate Swagger documentation for REST endpoints.
 */
@EnableConfigurationProperties(SwaggerProperties.class)
@Configuration
public class SwaggerConfig {
    
    @Autowired
    private SwaggerProperties swaggerProperties;
    
    @Autowired(required = false)
    private BuildProperties buildProperties;
    
    @Bean
    public OpenAPI springDocsOpenAPI() {
        String version = (buildProperties != null) ? buildProperties.getVersion() : "";
        return new OpenAPI()
                        .info(new Info().title(swaggerProperties.getTitle()).description(swaggerProperties.getDescription()).version(version)
                                        .license(new License().name(swaggerProperties.getLicenseName()).url(swaggerProperties.getLicenseUrl())))
                        .externalDocs(new ExternalDocumentation().description(swaggerProperties.getExternalDocsDesc())
                                        .url(swaggerProperties.getExternalDocsUrl()));
    }
}
