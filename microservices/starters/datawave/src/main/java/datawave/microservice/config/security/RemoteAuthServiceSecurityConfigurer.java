package datawave.microservice.config.security;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.lang.NonNull;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationProvider;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

import com.google.common.base.Preconditions;

import datawave.microservice.authorization.config.DatawaveSecurityProperties;
import datawave.microservice.authorization.jwt.JWTAuthenticationFilter;
import datawave.microservice.authorization.jwt.JWTAuthenticationProvider;
import datawave.microservice.authorization.preauth.ProxiedEntityX509Filter;
import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;

/**
 * Configures security for the spring boot application. This config is active only when the "remoteauth" profile has been specified, and this config overrides
 * that found in {@link JWTSecurityConfigurer} to authenticate users by making a remote call to a specified authorization service when no JWT is supplied in an
 * Authorization header.
 */
@Profile(RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE)
@Order(SecurityProperties.BASIC_AUTH_ORDER - 3)
@Configuration
@ConditionalOnWebApplication
public class RemoteAuthServiceSecurityConfigurer extends JWTSecurityConfigurer {
    private final DatawaveSecurityProperties securityProperties;
    private final AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> authenticationUserDetailsService;
    
    public RemoteAuthServiceSecurityConfigurer(DatawaveSecurityProperties securityProperties,
                    AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> authenticationUserDetailsService,
                    JWTAuthenticationProvider jwtAuthenticationProvider) {
        super(securityProperties, jwtAuthenticationProvider);
        this.securityProperties = securityProperties;
        this.authenticationUserDetailsService = authenticationUserDetailsService;
    }
    
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        
        super.configure(http);
        
        // The parent configures JWT-based security. Add an additional filter here to allow authentication based on the
        // X-ProxiedEntitiesChain/X-ProxiedIssuersChain headers that are supplied by trusted callers. These headers will
        // be used to make a remote call to the authorization service and retrieve the necessary credentials.
        ProxiedEntityX509Filter proxiedX509Filter = new ProxiedEntityX509Filter(securityProperties.isUseTrustedSubjectHeaders(),
                        securityProperties.isProxiedEntitiesRequired(), securityProperties.isIssuersRequired(), getAuthenticationEntryPoint());
        proxiedX509Filter.setAuthenticationManager(authenticationManager());
        proxiedX509Filter.setContinueFilterChainOnUnsuccessfulAuthentication(false);
        http.addFilterAfter(proxiedX509Filter, JWTAuthenticationFilter.class);
    }
    
    @Override
    protected void configure(@NonNull AuthenticationManagerBuilder auth) throws Exception {
        Preconditions.checkNotNull(auth);
        super.configure(auth);
        
        PreAuthenticatedAuthenticationProvider provider = new PreAuthenticatedAuthenticationProvider();
        provider.setPreAuthenticatedUserDetailsService(authenticationUserDetailsService);
        auth.authenticationProvider(provider);
    }
}
