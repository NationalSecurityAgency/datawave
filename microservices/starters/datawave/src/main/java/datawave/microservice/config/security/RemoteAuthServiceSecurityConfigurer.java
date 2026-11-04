package datawave.microservice.config.security;

import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationProvider;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

import datawave.microservice.authorization.config.DatawaveSecurityProperties;
import datawave.microservice.authorization.jwt.JWTAuthenticationFilter;
import datawave.microservice.authorization.jwt.JWTAuthenticationProvider;
import datawave.microservice.authorization.preauth.ProxiedEntityX509Filter;
import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;

/**
 * Configures security for the spring boot application. This config is active only when the "remoteauth" profile has been specified, and this config replaces
 * {@link JWTSecurityConfigurer} to authenticate users by making a remote call to a specified authorization service when no JWT is supplied in an Authorization
 * header.
 */
@Profile(RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE)
@Configuration
@ConditionalOnWebApplication
public class RemoteAuthServiceSecurityConfigurer extends AbstractJWTSecurityConfigurer {
    private final DatawaveSecurityProperties securityProperties;
    private final AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> authenticationUserDetailsService;

    public RemoteAuthServiceSecurityConfigurer(DatawaveSecurityProperties securityProperties,
                    AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> authenticationUserDetailsService,
                    JWTAuthenticationProvider jwtAuthenticationProvider) {
        super(securityProperties, jwtAuthenticationProvider);
        this.securityProperties = securityProperties;
        this.authenticationUserDetailsService = authenticationUserDetailsService;
    }

    @Bean
    @Order(SecurityProperties.BASIC_AUTH_ORDER - 3)
    public SecurityFilterChain remoteAuthSecurityFilterChain(HttpSecurity http) throws Exception {
        return buildSecurityFilterChain(http);
    }

    @Bean
    public WebSecurityCustomizer remoteAuthWebSecurityCustomizer() {
        return staticResourcesWebSecurityCustomizer();
    }

    @Override
    protected void configure(HttpSecurity http, AuthenticationManager authenticationManager) throws Exception {

        super.configure(http, authenticationManager);

        // The parent configures JWT-based security. Add an additional filter here to allow authentication based on the
        // X-ProxiedEntitiesChain/X-ProxiedIssuersChain headers that are supplied by trusted callers. These headers will
        // be used to make a remote call to the authorization service and retrieve the necessary credentials.
        ProxiedEntityX509Filter proxiedX509Filter = new ProxiedEntityX509Filter(securityProperties.isUseTrustedSubjectHeaders(),
                        securityProperties.isProxiedEntitiesRequired(), securityProperties.isIssuersRequired(), getAuthenticationEntryPoint());
        proxiedX509Filter.setAuthenticationManager(authenticationManager);
        proxiedX509Filter.setContinueFilterChainOnUnsuccessfulAuthentication(false);
        http.addFilterAfter(proxiedX509Filter, JWTAuthenticationFilter.class);
    }

    @Override
    protected List<AuthenticationProvider> getAuthenticationProviders() {
        List<AuthenticationProvider> providers = super.getAuthenticationProviders();

        PreAuthenticatedAuthenticationProvider provider = new PreAuthenticatedAuthenticationProvider();
        provider.setPreAuthenticatedUserDetailsService(authenticationUserDetailsService);
        providers.add(provider);
        return providers;
    }
}
