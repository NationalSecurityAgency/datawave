// package datawave.microservice.config.web;
//
// import com.google.common.base.Predicate;
// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
// import org.springframework.context.annotation.Bean;
// import org.springframework.context.annotation.Configuration;
// import springfox.documentation.RequestHandler;
// import springfox.documentation.builders.PathSelectors;
// import springfox.documentation.service.ApiInfo;
// import springfox.documentation.service.Contact;
// import springfox.documentation.spi.DocumentationType;
// import springfox.documentation.spring.web.plugins.Docket;
// import springfox.documentation.swagger2.annotations.EnableSwagger2;
//
// import java.util.Arrays;
//
/// **
// * Configuration to generate Swagger documentation for REST endpoints.
// */
// @Configuration
// @EnableSwagger2
// public class SwaggerConfig {
// @Bean
// @ConditionalOnMissingBean
// public Docket api(@Value("${spring.application.name}") String appName, @Value("${swagger.doc.packages:datawave.microservice}") String[] docPackages) {
//        // @formatter:off
//        return new Docket(DocumentationType.SWAGGER_2)
//            .select()
//            .apis(basePackages(docPackages))
//            .paths(PathSelectors.any())
//            .build()
//            .apiInfo(apiInfo(appName, SwaggerConfig.class.getPackage().getImplementationVersion()));
//        // @formatter:on
// }
//
// private ApiInfo apiInfo(String appName, String appVersion) {
// return new ApiInfo(appName + " API", "REST operations provided by the " + appName + " API", appVersion, null, (Contact) null, null, null);
// }
//
// private static Predicate<RequestHandler> basePackages(String[] packageNames) {
// return input -> Arrays.stream(packageNames).anyMatch(pkgName -> input.declaringClass().getPackage().getName().startsWith(pkgName));
// }
// }
