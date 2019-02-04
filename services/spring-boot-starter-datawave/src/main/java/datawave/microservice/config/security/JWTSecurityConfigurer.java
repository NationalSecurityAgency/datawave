package datawave.microservice.config.security;

import datawave.microservice.authorization.Http403ForbiddenEntryPoint;
import datawave.microservice.authorization.config.DatawaveSecurityProperties;
import datawave.microservice.authorization.jwt.JWTAuthenticationFilter;
import datawave.microservice.authorization.jwt.JWTAuthenticationProvider;
import datawave.security.authorization.SubjectIssuerDNPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.autoconfigure.security.servlet.EndpointRequest;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.preauth.x509.X509AuthenticationFilter;
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter.ReferrerPolicy;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * Configures security for the spring boot application. This config ensures that only listed certificate DNs can call us, and that we look up the proxied
 * users/servers using the supplied authorization service.
 */
@Order(SecurityProperties.BASIC_AUTH_ORDER - 2)
@Configuration
@EnableWebSecurity
@ConditionalOnWebApplication
@EnableGlobalMethodSecurity(prePostEnabled = true, jsr250Enabled = true)
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
        
        AllowedCallersFilter allowedCallersFilter = new AllowedCallersFilter(securityProperties);
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
        http.addFilterBefore(allowedCallersFilter, X509AuthenticationFilter.class);
        // Allow JWT authentication
        http.addFilterAfter(jwtFilter, AllowedCallersFilter.class);
    }
    
    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.authenticationProvider(jwtAuthenticationProvider);
    }
    
    protected AuthenticationEntryPoint getAuthenticationEntryPoint() {
        return authenticationEntryPoint;
    }
    
    protected static class AllowedCallersFilter extends OncePerRequestFilter {
        private final Logger logger = LoggerFactory.getLogger(getClass());
        private final DatawaveSecurityProperties securityProperties;
        
        public AllowedCallersFilter(DatawaveSecurityProperties securityProperties) {
            this.securityProperties = securityProperties;
        }
        
        @Override
        protected void doFilterInternal(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, FilterChain filterChain)
                        throws ServletException, IOException {
            // Extract the client certificate, and if one is provided, validate that the caller is allowed to talk to us.
            X509Certificate cert = extractClientCertificate(httpServletRequest);
            if (cert != null) {
                final SubjectIssuerDNPair dnPair = SubjectIssuerDNPair.of(cert.getSubjectX500Principal().getName(), cert.getIssuerX500Principal().getName());
                final String callerName = dnPair.toString();
                final List<String> allowedCallers = securityProperties.getAllowedCallers();
                if (securityProperties.isEnforceAllowedCallers() && !allowedCallers.contains(callerName)) {
                    logger.warn("Not allowing {} to talk since it is not in the allowed list of users {}", dnPair, allowedCallers);
                    throw new BadCredentialsException(dnPair + " is not authorized");
                }
            }
            // Continue the chain to handle any other filters
            filterChain.doFilter(httpServletRequest, httpServletResponse);
        }
        
        private X509Certificate extractClientCertificate(HttpServletRequest request) {
            X509Certificate[] certs = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
            
            if (certs != null && certs.length > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("X.509 client authentication certificate:" + certs[0]);
                }
                
                return certs[0];
            }
            
            if (logger.isDebugEnabled()) {
                logger.debug("No client certificate found in request.");
            }
            
            return null;
        }
    }
}
