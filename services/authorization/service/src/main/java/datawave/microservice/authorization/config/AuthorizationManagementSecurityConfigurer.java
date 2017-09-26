package datawave.microservice.authorization.config;

import datawave.microservice.authorization.jwt.JWTAuthenticationFilter;
import datawave.microservice.authorization.jwt.JWTAuthenticationProvider;
import datawave.microservice.authorization.preauth.ProxiedEntityX509Filter;
import datawave.microservice.config.security.ManagementSecurityConfigurer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.ManagementContextResolver;
import org.springframework.boot.actuate.autoconfigure.ManagementServerProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationProvider;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

@Order(99)
@Configuration
@ConditionalOnProperty(prefix = "management.security", name = "enabled", matchIfMissing = true)
public class AuthorizationManagementSecurityConfigurer extends ManagementSecurityConfigurer {
    private final DatawaveSecurityProperties securityProperties;
    private final AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> authenticationUserDetailsService;
    
    public AuthorizationManagementSecurityConfigurer(DatawaveSecurityProperties security, ManagementServerProperties management,
                    ObjectProvider<ManagementContextResolver> contextResolver,
                    AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> authenticationUserDetailsService,
                    JWTAuthenticationProvider jwtAuthenticationProvider) {
        super(security, management, contextResolver, jwtAuthenticationProvider);
        this.securityProperties = security;
        this.authenticationUserDetailsService = authenticationUserDetailsService;
    }
    
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        
        super.configure(http);
        
        // The parent configures JWT-based security. Add an additional filter here to allow authentication based on the
        // X-ProxiedEntitiesChain/X-ProxiedIssuersChain headers that are supplied by trusted callers.
        ProxiedEntityX509Filter proxiedX509Filter = new ProxiedEntityX509Filter(securityProperties.isUseTrustedSubjectHeaders(),
                        securityProperties.isProxiedEntitiesRequired(), securityProperties.isIssuersRequired(), getAuthenticationEntryPoint());
        proxiedX509Filter.setAuthenticationManager(authenticationManager());
        proxiedX509Filter.setContinueFilterChainOnUnsuccessfulAuthentication(false);
        http.addFilterAfter(proxiedX509Filter, JWTAuthenticationFilter.class);
    }
    
    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        super.configure(auth);
        
        PreAuthenticatedAuthenticationProvider provider = new PreAuthenticatedAuthenticationProvider();
        provider.setPreAuthenticatedUserDetailsService(authenticationUserDetailsService);
        auth.authenticationProvider(provider);
    }
}
