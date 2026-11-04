package datawave.microservice.config.security;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.security.web.SecurityFilterChain;

import datawave.microservice.authorization.config.DatawaveSecurityProperties;
import datawave.microservice.authorization.jwt.JWTAuthenticationProvider;
import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;

/**
 * Configures security for the spring boot application. This config ensures that only listed certificate DNs can call us, and that we look up the proxied
 * users/servers using the supplied authorization service.
 * <p>
 * This is the default configuration. It backs off when another {@link AbstractJWTSecurityConfigurer} is present (e.g., the authorization service's own
 * configurer), because Spring Security 6 rejects more than one filter chain that matches any request. Configuration class bean definitions are all registered
 * by component scanning before this condition is evaluated, so the check does not depend on scan order.
 */
@Profile("!" + RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE)
@Configuration
@ConditionalOnWebApplication
@ConditionalOnProperty(name = "security.jwt.enabled", matchIfMissing = true)
@ConditionalOnMissingBean(value = AbstractJWTSecurityConfigurer.class, ignored = JWTSecurityConfigurer.class)
public class JWTSecurityConfigurer extends AbstractJWTSecurityConfigurer {

    public JWTSecurityConfigurer(DatawaveSecurityProperties securityProperties, JWTAuthenticationProvider jwtAuthenticationProvider) {
        super(securityProperties, jwtAuthenticationProvider);
    }

    @Bean
    @Order(SecurityProperties.BASIC_AUTH_ORDER - 2)
    public SecurityFilterChain jwtSecurityFilterChain(HttpSecurity http) throws Exception {
        return buildSecurityFilterChain(http);
    }

    @Bean
    public WebSecurityCustomizer jwtWebSecurityCustomizer() {
        return staticResourcesWebSecurityCustomizer();
    }

}
