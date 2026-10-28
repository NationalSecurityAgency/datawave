package datawave.configuration.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

@Configuration
public class SpringCDIConfiguration {

    @Bean
    // This is a fall-back bean that can be used to get a SpringContextReport if the
    // ClasspathXmlApplicationContext fails to load.
    public SpringContextReport springContextReport() {
        return new SpringContextReport();
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        PropertySourcesPlaceholderConfigurer pspc = new PropertySourcesPlaceholderConfigurer();
        pspc.setIgnoreUnresolvablePlaceholders(true);
        return pspc;
    }
}
