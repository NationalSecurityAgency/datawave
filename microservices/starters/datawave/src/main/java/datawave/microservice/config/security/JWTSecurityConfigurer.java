package datawave.microservice.config.security;

import org.springframework.boot.actuate.autoconfigure.security.servlet.EndpointRequest;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.boot.autoconfigure.security.servlet.PathRequest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.lang.NonNull;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.preauth.AbstractPreAuthenticatedProcessingFilter;
import org.springframework.security.web.authentication.preauth.x509.X509AuthenticationFilter;
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter.ReferrerPolicy;

import com.google.common.base.Preconditions;

import datawave.microservice.authorization.Http403ForbiddenEntryPoint;
import datawave.microservice.authorization.config.DatawaveSecurityProperties;
import datawave.microservice.authorization.jwt.JWTAuthenticationFilter;
import datawave.microservice.authorization.jwt.JWTAuthenticationProvider;
import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;

/**
 * Configures security for the spring boot application. This config ensures that only listed certificate DNs can call us, and that we look up the proxied
 * users/servers using the supplied authorization service.
 */
@Profile("!" + RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE)
@Order(SecurityProperties.BASIC_AUTH_ORDER - 2)
@Configuration
@ConditionalOnWebApplication
@ConditionalOnProperty(name = "security.jwt.enabled", matchIfMissing = true)
public class JWTSecurityConfigurer extends WebSecurityConfigurerAdapter {
    private final DatawaveSecurityProperties securityProperties;
    private final JWTAuthenticationProvider jwtAuthenticationProvider;
    private final AuthenticationEntryPoint authenticationEntryPoint;
    
    public JWTSecurityConfigurer(DatawaveSecurityProperties securityProperties, JWTAuthenticationProvider jwtAuthenticationProvider) {
        this.securityProperties = securityProperties;
        this.jwtAuthenticationProvider = jwtAuthenticationProvider;
        this.authenticationEntryPoint = new Http403ForbiddenEntryPoint();
    }
    
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        // Allow unauthenticated access to actuator info and health endpoints.
        http.authorizeRequests().requestMatchers(EndpointRequest.to("info", "health")).permitAll();
        
        // Require users to have one of the defined manager roles for accessing any actuator endpoint other
        // than info or health (see above).
        if (!securityProperties.getManagerRoles().isEmpty()) {
            http = http.authorizeRequests().requestMatchers(EndpointRequest.toAnyEndpoint())
                            .hasAnyAuthority(securityProperties.getManagerRoles().toArray(new String[0])).and();
        }
        
        // Apply this configuration to all requests...
        http = http.requestMatchers().anyRequest().and();
        
        if (securityProperties.isRequireSsl()) {
            http.requiresChannel().anyRequest().requiresSecure();
        }
        
        JWTAuthenticationFilter jwtFilter = new JWTAuthenticationFilter(false, authenticationManager(), authenticationEntryPoint);
        
        // Allow CORS requests
        http.cors();
        // Disable CSRF protection since we're not using cookies anyway
        http.csrf().disable();
        // Send the Referrer-Policy header in the response
        http.headers().referrerPolicy(ReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN);
        // Set the Content-Security-Policy header
        http.headers().contentSecurityPolicy("frame-ancestors 'self'");
        // All requests (subject to the matcher patterns above) must be authenticated
        http.authorizeRequests().anyRequest().fullyAuthenticated();
        // Ensure that we never create a session--we always want to get the latest information from the certificate/headers
        http.sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS);
        // Send unauthenticated people a 403 response without redirecting to a failure page
        http.exceptionHandling().authenticationEntryPoint(authenticationEntryPoint).accessDeniedPage(null);
        // Extract principal information from incoming certificates so that we can limit access to specific DNs
        AllowedCallersFilter allowedCallersFilter = getAllowedCallersFilter(securityProperties);
        http.addFilterBefore(allowedCallersFilter, X509AuthenticationFilter.class);
        // Allow JWT authentication
        http.addFilterAfter(jwtFilter, X509AuthenticationFilter.class);
        // Block users with denied-access role
        DeniedAccessRoleFilter deniedAccessRoleFilter = new DeniedAccessRoleFilter(securityProperties);
        http.addFilterAfter(deniedAccessRoleFilter, AbstractPreAuthenticatedProcessingFilter.class);
    }
    
    protected AllowedCallersFilter getAllowedCallersFilter(DatawaveSecurityProperties securityProperties) {
        return new AllowedCallersFilter(securityProperties, authenticationEntryPoint);
    }
    
    @Override
    protected void configure(@NonNull AuthenticationManagerBuilder auth) throws Exception {
        Preconditions.checkNotNull(auth);
        auth.authenticationProvider(jwtAuthenticationProvider);
    }
    
    /**
     * Configures web security to allow access to static resources without authentication.
     *
     * @param web
     *            the {@link WebSecurity} to configure
     * @throws Exception
     *             if there is any problem configuring the web security
     */
    @Override
    public void configure(WebSecurity web) throws Exception {
        super.configure(web);
        web.ignoring().requestMatchers(PathRequest.toStaticResources().atCommonLocations());
    }
    
    protected AuthenticationEntryPoint getAuthenticationEntryPoint() {
        return authenticationEntryPoint;
    }
    
}
