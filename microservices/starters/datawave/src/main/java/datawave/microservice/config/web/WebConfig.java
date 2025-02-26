package datawave.microservice.config.web;

import java.util.List;

import javax.servlet.DispatcherType;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.reactive.config.WebFluxConfigurer;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;

import datawave.microservice.config.web.DatawaveServerProperties.Cors;
import datawave.microservice.config.web.filter.ResponseHeaderServletFilter;
import datawave.microservice.config.web.filter.ResponseHeaderWebFilter;
import datawave.microservice.http.converter.html.BannerProvider;
import datawave.microservice.http.converter.html.HtmlProviderHttpMessageConverter;
import datawave.microservice.http.converter.html.VoidResponseHttpMessageConverter;
import datawave.microservice.http.converter.protostuff.ProtostuffHttpMessageConverter;
import datawave.webservice.HtmlProvider;
import datawave.webservice.result.VoidResponse;

/**
 * Web configuration for Datawave microservices. TODO: We have only setup configuration here for a servlet-based web application. Additional work needs to be
 * done for a reactive application.
 */
@Configuration
@EnableConfigurationProperties(DatawaveServerProperties.class)
public class WebConfig {
    /**
     * Creates a {@link JaxbAnnotationModule} bean, which will be added automatically to any {@link ObjectMapper} created by Spring. The
     * {@link JaxbAnnotationModule} causes Jackson to honor JAX-B annotations on object when producing JSON. This default behavior can be disabled by setting
     * the property {@code spring.jackson.module.jaxb-annotation-module} to {@code false}.
     * 
     * @return a new {@link JaxbAnnotationModule}
     */
    @Bean
    @ConditionalOnProperty(name = "spring.jackson.module.jaxb-annotation-module", havingValue = "true", matchIfMissing = true)
    public JaxbAnnotationModule jaxbAnnotationModule() {
        return new JaxbAnnotationModule();
    }
    
    /**
     * Enables by default the Jackson object mapper feature {@link MapperFeature#USE_WRAPPER_NAME_AS_PROPERTY_NAME}. For annotated objects that use
     * {@link javax.xml.bind.annotation.XmlElementWrapper}, this will use the specified name as the JSON property name rather than the value of
     * {@link javax.xml.bind.annotation.XmlElement}. Disable by setting the property {@code spring.jackson.mapper.use-wrapper-name-as-property-name} to
     * {@code false}.
     *
     * @return a {@link Jackson2ObjectMapperBuilderCustomizer} that enables the {@link MapperFeature#USE_WRAPPER_NAME_AS_PROPERTY_NAME} Jackson object mapper
     *         feature
     */
    @Bean
    @ConditionalOnProperty(name = "spring.jackson.mapper.use-wrapper-name-as-property-name", havingValue = "true", matchIfMissing = true)
    public Jackson2ObjectMapperBuilderCustomizer datawaveJacksonCustomizer() {
        return c -> c.featuresToEnable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
    }
    
    /**
     * Creates a {@link WebMvcConfigurer} that applies CORS configuration as defined in the {@link Cors} properties.
     *
     * @param serverProperties
     *            the {@link DatawaveServerProperties} from which to retrieve {@link Cors} properties
     * @return a {@link WebMvcConfigurer} that applies CORS settings for a servlet application
     */
    @Bean
    public WebMvcConfigurer corsConfiguration(DatawaveServerProperties serverProperties) {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                Cors cors = serverProperties.getCors();
                // @formatter:off
                cors.getCorsPaths().forEach(p -> registry.addMapping(p)
                        .allowedOriginPatterns(cors.getAllowedOriginPatterns())
                        .allowedMethods(cors.getAllowedMethods())
                        .allowedHeaders(cors.getAllowedHeaders())
                        .allowCredentials(cors.isAllowCredentials())
                        .maxAge(cors.getMaxAge()));
                // @formatter:on
            }
        };
    }
    
    /**
     * Creates a {@link WebFluxConfigurer} that applies CORS configuration as defined in the {@link Cors} properties.
     *
     * @param serverProperties
     *            the {@link DatawaveServerProperties} from which to retrieve {@link Cors} properties
     * @return a {@link WebFluxConfigurer} that applies CORS settings for a webflux application
     */
    @Bean
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
    public WebFluxConfigurer webFluxCorsConfigurer(DatawaveServerProperties serverProperties) {
        return new WebFluxConfigurer() {
            @Override
            public void addCorsMappings(org.springframework.web.reactive.config.CorsRegistry registry) {
                Cors cors = serverProperties.getCors();
                // @formatter:off
                cors.getCorsPaths().forEach(p -> registry.addMapping(p)
                        .allowedOriginPatterns(cors.getAllowedOriginPatterns())
                        .allowedMethods(cors.getAllowedMethods())
                        .allowedHeaders(cors.getAllowedHeaders())
                        .allowCredentials(cors.isAllowCredentials())
                        .maxAge(cors.getMaxAge()));
                // @formatter:on
            }
        };
    }
    
    /**
     * Creates a {@link WebMvcConfigurer} that adds {@link HttpMessageConverter}s to handle protostuff {@link io.protostuff.Message} responses,
     * {@link HtmlProvider} responses, and {@link VoidResponse} responses.
     *
     * @param serverProperties
     *            the {@link DatawaveServerProperties} from which to retrieve the static CSS location for HTML responses.
     * @return a {@link WebMvcConfigurer} that adds custom {@link HttpMessageConverter}s.
     */
    @Bean
    public WebMvcConfigurer messageConverterConfiguration(final DatawaveServerProperties serverProperties, ObjectProvider<BannerProvider> bannerProvider) {
        return new WebMvcConfigurer() {
            @Override
            public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
                converters.add(new ProtostuffHttpMessageConverter());
                converters.add(new VoidResponseHttpMessageConverter(serverProperties, bannerProvider.getIfAvailable()));
                converters.add(new HtmlProviderHttpMessageConverter(serverProperties, bannerProvider.getIfAvailable()));
            }
        };
    }
    
    /**
     * Creates a {@link FilterRegistrationBean} that registers our custom filter that adds headers to HTTP responses.
     *
     * @return a {@link FilterRegistrationBean} that registers a {@link ResponseHeaderServletFilter}
     */
    @Bean
    @ConditionalOnClass(DispatcherServlet.class)
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    public FilterRegistrationBean<ResponseHeaderServletFilter> responseHeaderFilter(@Value("${system.name:UNKNOWN}") String systemName) {
        ResponseHeaderServletFilter filter = new ResponseHeaderServletFilter(systemName);
        FilterRegistrationBean<ResponseHeaderServletFilter> registration = new FilterRegistrationBean<>(filter);
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 1);
        registration.setDispatcherTypes(DispatcherType.REQUEST, DispatcherType.ASYNC);
        return registration;
    }
    
    /**
     * Creates a {@link org.springframework.web.server.WebFilter} that adds headers to HTTP responses for reactive applications.
     *
     * @return a new {@link ResponseHeaderWebFilter}
     */
    @Bean
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
    public ResponseHeaderWebFilter responseHeaderWebFilter(@Value("${system.name:UNKNOWN}") String systemName) {
        ResponseHeaderWebFilter filter = new ResponseHeaderWebFilter(systemName);
        filter.setOrder(Ordered.HIGHEST_PRECEDENCE + 1);
        return filter;
    }
}
