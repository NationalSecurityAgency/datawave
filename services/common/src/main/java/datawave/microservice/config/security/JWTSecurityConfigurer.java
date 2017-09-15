package datawave.microservice.config.security;

import datawave.microservice.authorization.Http403ForbiddenEntryPoint;
import datawave.microservice.authorization.config.DatawaveSecurityProperties;
import datawave.microservice.authorization.jwt.JWTAuthenticationProvider;
import datawave.security.authorization.SubjectIssuerDNPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.boot.autoconfigure.security.SpringBootWebSecurityConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationProvider;
import org.springframework.security.web.authentication.preauth.x509.X509AuthenticationFilter;
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter.ReferrerPolicy;

import java.util.Collections;
import java.util.List;

/**
 * Configures security for the spring boot application. This config ensures that only listed certificate DNs can call us, and that we look up the proxied
 * users/servers using the supplied authorization service.
 */
@Order(SecurityProperties.ACCESS_OVERRIDE_ORDER)
@Configuration
@EnableWebSecurity
public class JWTSecurityConfigurer extends WebSecurityConfigurerAdapter {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private final DatawaveSecurityProperties securityProperties;
    private final JWTAuthenticationProvider jwtAuthenticationProvider;
    
    public JWTSecurityConfigurer(DatawaveSecurityProperties securityProperties, JWTAuthenticationProvider jwtAuthenticationProvider) {
        this.securityProperties = securityProperties;
        this.jwtAuthenticationProvider = jwtAuthenticationProvider;
    }
    
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        // Apply this configuration to all requests...
        http = http.requestMatchers().anyRequest().and();
        
        if (securityProperties.isRequireSsl()) {
            http.requiresChannel().anyRequest().requiresSecure();
        }
        if (!securityProperties.isEnableCsrf()) {
            http.csrf().disable();
        }
        
        Http403ForbiddenEntryPoint authenticationEntryPoint = new Http403ForbiddenEntryPoint();
        
        X509AuthenticationFilter x509Filter = new X509AuthenticationFilter();
        x509Filter.setAuthenticationManager(authenticationManager());
        x509Filter.setPrincipalExtractor(cert -> SubjectIssuerDNPair.of(cert.getSubjectX500Principal().getName(), cert.getIssuerX500Principal().getName()));
        
        // Allow CORS requests
        http.cors();
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
        http.addFilter(x509Filter);
        
        SpringBootWebSecurityConfiguration.configureHeaders(http.headers(), securityProperties.getHeaders());
    }
    
    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        PreAuthenticatedAuthenticationProvider x509AuthenticationProvider = new PreAuthenticatedAuthenticationProvider();
        x509AuthenticationProvider.setPreAuthenticatedUserDetailsService(token -> {
            SubjectIssuerDNPair sip = (SubjectIssuerDNPair) token.getPrincipal();
            String callerName = sip.toString();
            final List<String> allowedCallers = securityProperties.getAllowedCallers();
            if (securityProperties.isEnforceAllowedCallers() && !allowedCallers.contains(callerName)) {
                logger.warn("Not allowing {} to talk since it is not in the allowed list of users {}", sip, allowedCallers);
                throw new BadCredentialsException(sip + " is not authorized");
            }
            return new User(callerName, "", Collections.emptyList());
        });
        
        auth.authenticationProvider(x509AuthenticationProvider);
        auth.authenticationProvider(jwtAuthenticationProvider);
    }
}
