package datawave.microservice.config.security;

import datawave.microservice.authorization.Http403ForbiddenEntryPoint;
import datawave.microservice.authorization.config.DatawaveSecurityProperties;
import datawave.microservice.authorization.jwt.JWTAuthenticationFilter;
import datawave.microservice.authorization.jwt.JWTAuthenticationProvider;
import datawave.microservice.authorization.preauth.ProxiedEntityX509Filter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.ManagementContextResolver;
import org.springframework.boot.actuate.autoconfigure.ManagementServerProperties;
import org.springframework.boot.actuate.endpoint.mvc.EndpointHandlerMapping;
import org.springframework.boot.actuate.endpoint.mvc.MvcEndpoint;
import org.springframework.boot.autoconfigure.security.SpringBootWebSecurityConfiguration;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.annotation.web.configurers.ExpressionUrlAuthorizationConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationProvider;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.security.web.authentication.preauth.x509.X509AuthenticationFilter;
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.security.web.util.matcher.AnyRequestMatcher;
import org.springframework.security.web.util.matcher.NegatedRequestMatcher;
import org.springframework.security.web.util.matcher.OrRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Configures security for managment endpoints. This overrides the configuration found in
 * {@link org.springframework.boot.actuate.autoconfigure.ManagementWebSecurityAutoConfiguration} such that sensitive endpoints require X509 or JWT
 * authentication.
 */
public class ManagementSecurityConfigurer extends WebSecurityConfigurerAdapter {
    private static final String[] NO_PATHS = new String[0];
    private static final RequestMatcher MATCH_NONE = new NegatedRequestMatcher(AnyRequestMatcher.INSTANCE);
    
    private final DatawaveSecurityProperties security;
    private final ManagementServerProperties management;
    private final ManagementContextResolver contextResolver;
    private final JWTAuthenticationProvider jwtAuthenticationProvider;
    private final AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> authenticationUserDetailsService;
    
    public ManagementSecurityConfigurer(DatawaveSecurityProperties security, ManagementServerProperties management,
                    ObjectProvider<ManagementContextResolver> contextResolver, JWTAuthenticationProvider jwtAuthenticationProvider,
                    AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> authenticationUserDetailsService) {
        this.security = security;
        this.management = management;
        this.contextResolver = contextResolver.getIfAvailable();
        this.jwtAuthenticationProvider = jwtAuthenticationProvider;
        this.authenticationUserDetailsService = authenticationUserDetailsService;
    }
    
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        // secure endpoints
        RequestMatcher matcher = getRequestMatcher();
        if (matcher != null) {
            // Always protect them if present
            if (this.security.isRequireSsl()) {
                http.requiresChannel().anyRequest().requiresSecure();
            }
            AuthenticationEntryPoint authenticationEntryPoint = new Http403ForbiddenEntryPoint();
            AuthenticationManager authenticationManager = authenticationManager();
            
            ProxiedEntityX509Filter proxiedX509Filter = new ProxiedEntityX509Filter(security.isProxiedEntitiesRequired(), security.isIssuersRequired(),
                            authenticationEntryPoint);
            proxiedX509Filter.setAuthenticationManager(authenticationManager);
            
            JWTAuthenticationFilter jwtFilter = new JWTAuthenticationFilter(false, authenticationManager, authenticationEntryPoint);
            
            // Any request to the actuator interfaces allows administrators, and all other requests (subject to the matcher patterns above) must be
            // authenticated.
            
            // Send unauthenticated people a 403 response without redirecting to a failure page
            http.exceptionHandling().authenticationEntryPoint(authenticationEntryPoint).accessDeniedPage(null);
            // Match all requests for actuator endpoints
            http.requestMatcher(matcher);
            // ... but permitAll() for the non-sensitive ones
            configurePermittedRequests(http.authorizeRequests());
            // No cookies for management endpoints by default
            http.csrf().disable();
            // Send the Referrer-Policy header in the response
            http.headers().referrerPolicy(ReferrerPolicyHeaderWriter.ReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN);
            // Set the Content-Security-Policy header
            http.headers().contentSecurityPolicy("frame-ancestors 'self'");
            // Ensure that we never create a session--we always want to get the latest information from the certificate/headers
            http.sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS);
            // Pull certificate information from X509 certificates and/or headers
            http.addFilterBefore(proxiedX509Filter, X509AuthenticationFilter.class);
            // Allow JWT header authentication too
            http.addFilterBefore(jwtFilter, ProxiedEntityX509Filter.class);
            SpringBootWebSecurityConfiguration.configureHeaders(http.headers(), security.getHeaders());
        }
    }
    
    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        PreAuthenticatedAuthenticationProvider provider = new PreAuthenticatedAuthenticationProvider();
        provider.setPreAuthenticatedUserDetailsService(authenticationUserDetailsService);
        auth.authenticationProvider(jwtAuthenticationProvider);
        auth.authenticationProvider(provider);
    }
    
    private RequestMatcher getRequestMatcher() {
        if (this.management.getSecurity().isEnabled()) {
            return LazyEndpointPathRequestMatcher.getRequestMatcher(this.contextResolver);
        }
        return null;
    }
    
    private void configurePermittedRequests(ExpressionUrlAuthorizationConfigurer<HttpSecurity>.ExpressionInterceptUrlRegistry requests) {
        requests.requestMatchers(new LazyEndpointPathRequestMatcher(this.contextResolver, EndpointPaths.SENSITIVE)).authenticated();
        // Permit access to the non-sensitive endpoints
        requests.requestMatchers(new LazyEndpointPathRequestMatcher(this.contextResolver, EndpointPaths.NON_SENSITIVE)).permitAll();
    }
    
    private enum EndpointPaths {
        
        ALL,
        
        NON_SENSITIVE {
            
            @Override
            protected boolean isIncluded(MvcEndpoint endpoint) {
                return !endpoint.isSensitive();
            }
            
        },
        
        SENSITIVE {
            
            @Override
            protected boolean isIncluded(MvcEndpoint endpoint) {
                return endpoint.isSensitive();
            }
            
        };
        
        public String[] getPaths(EndpointHandlerMapping endpointHandlerMapping) {
            if (endpointHandlerMapping == null) {
                return NO_PATHS;
            }
            Set<? extends MvcEndpoint> endpoints = endpointHandlerMapping.getEndpoints();
            Set<String> paths = new LinkedHashSet<>(endpoints.size());
            for (MvcEndpoint endpoint : endpoints) {
                if (isIncluded(endpoint)) {
                    String path = endpointHandlerMapping.getPath(endpoint.getPath());
                    paths.add(path);
                    if (!path.equals("")) {
                        paths.add(path + "/**");
                        // Add Spring MVC-generated additional paths
                        paths.add(path + ".*");
                    }
                    paths.add(path + "/");
                }
            }
            return paths.toArray(new String[paths.size()]);
        }
        
        protected boolean isIncluded(MvcEndpoint endpoint) {
            return true;
        }
        
    }
    
    private static class LazyEndpointPathRequestMatcher implements RequestMatcher {
        
        private final EndpointPaths endpointPaths;
        
        private final ManagementContextResolver contextResolver;
        
        private RequestMatcher delegate;
        
        public static RequestMatcher getRequestMatcher(ManagementContextResolver contextResolver) {
            if (contextResolver == null) {
                return null;
            }
            ManagementServerProperties management = contextResolver.getApplicationContext().getBean(ManagementServerProperties.class);
            ServerProperties server = contextResolver.getApplicationContext().getBean(ServerProperties.class);
            String path = management.getContextPath();
            if (StringUtils.hasText(path)) {
                return new AntPathRequestMatcher(server.getPath(path) + "/**");
            }
            // Match everything, including the sensitive and non-sensitive paths
            return new LazyEndpointPathRequestMatcher(contextResolver, EndpointPaths.ALL);
        }
        
        LazyEndpointPathRequestMatcher(ManagementContextResolver contextResolver, EndpointPaths endpointPaths) {
            this.contextResolver = contextResolver;
            this.endpointPaths = endpointPaths;
        }
        
        @Override
        public boolean matches(HttpServletRequest request) {
            if (this.delegate == null) {
                this.delegate = createDelegate();
            }
            return this.delegate.matches(request);
        }
        
        private RequestMatcher createDelegate() {
            ServerProperties server = this.contextResolver.getApplicationContext().getBean(ServerProperties.class);
            List<RequestMatcher> matchers = new ArrayList<RequestMatcher>();
            EndpointHandlerMapping endpointHandlerMapping = getRequiredEndpointHandlerMapping();
            for (String path : this.endpointPaths.getPaths(endpointHandlerMapping)) {
                matchers.add(new AntPathRequestMatcher(server.getPath(path)));
            }
            return (matchers.isEmpty() ? MATCH_NONE : new OrRequestMatcher(matchers));
        }
        
        private EndpointHandlerMapping getRequiredEndpointHandlerMapping() {
            EndpointHandlerMapping endpointHandlerMapping = null;
            ApplicationContext context = this.contextResolver.getApplicationContext();
            if (context.getBeanNamesForType(EndpointHandlerMapping.class).length > 0) {
                endpointHandlerMapping = context.getBean(EndpointHandlerMapping.class);
            }
            if (endpointHandlerMapping == null) {
                // Maybe there are actually no endpoints (e.g. management.port=-1)
                endpointHandlerMapping = new EndpointHandlerMapping(Collections.emptySet());
            }
            return endpointHandlerMapping;
        }
        
    }
    
}
