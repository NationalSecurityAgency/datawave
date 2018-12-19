package datawave.microservice.config.web;

import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import datawave.microservice.config.web.DatawaveServerProperties.Cors;
import datawave.microservice.config.web.filter.ResponseHeaderFilter;
import datawave.microservice.http.converter.html.HtmlProviderHttpMessageConverter;
import datawave.microservice.http.converter.html.VoidResponseHttpMessageConverter;
import datawave.microservice.http.converter.protostuff.ProtostuffHttpMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.servlet.DispatcherType;
import java.util.List;

@Configuration
public class WebConfig {
    /**
     * Creates a {@link JaxbAnnotationModule} bean, which will be added automatically to any {@link com.fasterxml.jackson.databind.ObjectMapper} created by
     * Spring.
     * 
     * @return a new {@link JaxbAnnotationModule}
     */
    @Bean
    public JaxbAnnotationModule jaxbAnnotationModule() {
        return new JaxbAnnotationModule();
    }
    
    /**
     * Creates a {@link WebMvcConfigurer} that applies CORS configuration as defined in the {@link Cors} properties.
     *
     * @param serverProperties
     *            the {@link DatawaveServerProperties} from which to retrieve {@link Cors} properties
     * @return a {@link WebMvcConfigurer} that applies CORS settings
     */
    @Bean
    public WebMvcConfigurer corsConfiguration(final DatawaveServerProperties serverProperties) {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                Cors cors = serverProperties.getCors();
                // @formatter:off
                cors.getCorsPaths().forEach(p -> registry.addMapping(p)
                        .allowedOrigins(cors.getAllowedOrigins())
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
     * {@link datawave.webservice.HtmlProvider} responses, and {@link datawave.webservice.result.VoidResponse} responses.
     *
     * @param serverProperties
     *            the {@link DatawaveServerProperties} from which to retrieve the static CSS location for HTML responses.
     * @return a {@link WebMvcConfigurer} that adds custom {@link HttpMessageConverter}s.
     */
    @Bean
    public WebMvcConfigurer messageConverterConfiguration(final DatawaveServerProperties serverProperties) {
        return new WebMvcConfigurer() {
            @Override
            public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
                converters.add(new ProtostuffHttpMessageConverter());
                converters.add(new VoidResponseHttpMessageConverter(serverProperties));
                converters.add(new HtmlProviderHttpMessageConverter(serverProperties));
            }
        };
    }
    
    /**
     * Creates a {@link FilterRegistrationBean} that registers our custom filter that adds headers to HTTP responses.
     *
     * @return a {@link FilterRegistrationBean} that registers a {@link ResponseHeaderFilter}
     */
    @Bean
    @ConditionalOnClass(DispatcherServlet.class)
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    public FilterRegistrationBean<ResponseHeaderFilter> responseHeaderFilter(@Value("${system.name:UNKNOWN}") String systemName) {
        ResponseHeaderFilter filter = new ResponseHeaderFilter(systemName);
        FilterRegistrationBean<ResponseHeaderFilter> registration = new FilterRegistrationBean<>(filter);
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 1);
        registration.setDispatcherTypes(DispatcherType.REQUEST, DispatcherType.ASYNC);
        return registration;
    }
}
