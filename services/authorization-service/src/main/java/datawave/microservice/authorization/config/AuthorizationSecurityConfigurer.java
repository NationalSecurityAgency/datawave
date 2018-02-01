package datawave.microservice.authorization.config;

import datawave.microservice.authorization.jwt.JWTAuthenticationFilter;
import datawave.microservice.authorization.jwt.JWTAuthenticationProvider;
import datawave.microservice.authorization.preauth.ProxiedEntityX509Filter;
import datawave.microservice.config.security.JWTSecurityConfigurer;
import org.springframework.boot.actuate.autoconfigure.ManagementServerProperties;
import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationProvider;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

/**
 * Configures security for the spring boot application. This configuration ensures that only allowed callers can talk to us, and the caller can supply either a
 * signed JSON Web Token containing the authorization credentials or can authenticate by using the X-ProxiedEntitiesChain and X-ProxiedIssuersChain headers that
 * are supplied by trusted callers.
 * <p>
 * Note that for authentication purposes, it is assumed that another service is authenticating on behalf of a user, and therefore <em>only</em> the
 * X-ProxiedEntitiesChain/X-ProxiedIssuersChain values are used to calculate the authenticated principal and the incoming certificate is only used to determine
 * if the caller is trusted. This behavior can be change if {@link DatawaveSecurityProperties#isProxiedEntitiesRequired()} is set to false. In that case, if the
 * caller supplies no X-ProxiedEntitiesChain/X-ProxiedIssuersChain header, the incoming certificate subject and issuer are copied into the
 * X-ProxiedEntitiesChain/X-ProxiedIssuersChain header so that the caller is proxying for itself. This is a convenience used for testing when one would like a
 * user to be able to directly access the authorization service (e.g., from a web browser).
 */
@Order(SecurityProperties.ACCESS_OVERRIDE_ORDER - 1)
@Configuration
@EnableWebSecurity
@EnableCaching
@EnableGlobalMethodSecurity(prePostEnabled = true, jsr250Enabled = true)
public class AuthorizationSecurityConfigurer extends JWTSecurityConfigurer {
    private final DatawaveSecurityProperties securityProperties;
    private final AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> authenticationUserDetailsService;
    
    public AuthorizationSecurityConfigurer(ManagementServerProperties managementServerProperties, DatawaveSecurityProperties securityProperties,
                    AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> authenticationUserDetailsService,
                    JWTAuthenticationProvider jwtAuthenticationProvider) {
        super(managementServerProperties, securityProperties, jwtAuthenticationProvider);
        this.securityProperties = securityProperties;
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
