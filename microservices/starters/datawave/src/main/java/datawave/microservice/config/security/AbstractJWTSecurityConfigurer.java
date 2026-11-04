package datawave.microservice.config.security;

import static org.springframework.security.config.Customizer.withDefaults;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.actuate.autoconfigure.security.servlet.EndpointRequest;
import org.springframework.boot.autoconfigure.security.servlet.PathRequest;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.preauth.AbstractPreAuthenticatedProcessingFilter;
import org.springframework.security.web.authentication.preauth.x509.X509AuthenticationFilter;
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter.ReferrerPolicy;

import datawave.microservice.authorization.Http403ForbiddenEntryPoint;
import datawave.microservice.authorization.config.DatawaveSecurityProperties;
import datawave.microservice.authorization.jwt.JWTAuthenticationFilter;
import datawave.microservice.authorization.jwt.JWTAuthenticationProvider;

/**
 * Shared security configuration for the spring boot application. This config ensures that only listed certificate DNs can call us, and that we look up the
 * proxied users/servers using the supplied authorization service.
 * <p>
 * Each concrete subclass registers its own {@link SecurityFilterChain} bean (with its own name and order) by calling {@link #buildSecurityFilterChain}. The
 * bean methods live in the subclasses rather than here so that a service can have more than one of these configurations active without the inherited bean
 * names colliding.
 */
public abstract class AbstractJWTSecurityConfigurer {
    private final DatawaveSecurityProperties securityProperties;
    private final JWTAuthenticationProvider jwtAuthenticationProvider;
    private final AuthenticationEntryPoint authenticationEntryPoint;

    protected AbstractJWTSecurityConfigurer(DatawaveSecurityProperties securityProperties, JWTAuthenticationProvider jwtAuthenticationProvider) {
        this.securityProperties = securityProperties;
        this.jwtAuthenticationProvider = jwtAuthenticationProvider;
        this.authenticationEntryPoint = new Http403ForbiddenEntryPoint();
    }

    /**
     * Builds the security filter chain for this configuration. The {@link AuthenticationManager} used by the chain's filters is built from
     * {@link #getAuthenticationProviders()}.
     *
     * @param http
     *            the {@link HttpSecurity} to configure
     * @return the built filter chain
     * @throws Exception
     *             if there is any problem configuring the http security
     */
    protected SecurityFilterChain buildSecurityFilterChain(HttpSecurity http) throws Exception {
        AuthenticationManager authenticationManager = new ProviderManager(getAuthenticationProviders());
        http.authenticationManager(authenticationManager);
        configure(http, authenticationManager);
        return http.build();
    }

    protected void configure(HttpSecurity http, AuthenticationManager authenticationManager) throws Exception {
        // Allow unauthenticated access to actuator info and health endpoints.
        http.authorizeHttpRequests(requests -> requests.requestMatchers(EndpointRequest.to("info", "health")).permitAll());

        // Require users to have one of the defined manager roles for accessing any actuator endpoint other
        // than info or health (see above).
        if (!securityProperties.getManagerRoles().isEmpty()) {
            http.authorizeHttpRequests(requests -> requests.requestMatchers(EndpointRequest.toAnyEndpoint())
                            .hasAnyAuthority(securityProperties.getManagerRoles().toArray(new String[0])));
        }

        // This configuration applies to all requests (the default security matcher matches any request).

        if (securityProperties.isRequireSsl()) {
            http.requiresChannel(channel -> channel.anyRequest().requiresSecure());
        }

        JWTAuthenticationFilter jwtFilter = new JWTAuthenticationFilter(false, authenticationManager, authenticationEntryPoint);

        // Allow CORS requests
        http.cors(withDefaults());
        // Disable CSRF protection since we're not using cookies anyway
        http.csrf(csrf -> csrf.disable());
        // Send the Referrer-Policy header in the response
        http.headers(headers -> headers.referrerPolicy(policy -> policy.policy(ReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN)));
        // Set the Content-Security-Policy header
        http.headers(headers -> headers.contentSecurityPolicy(policy -> policy.policyDirectives("frame-ancestors 'self'")));
        // All requests (subject to the matcher patterns above) must be authenticated
        http.authorizeHttpRequests(requests -> requests.anyRequest().fullyAuthenticated());
        // Ensure that we never create a session--we always want to get the latest information from the certificate/headers
        http.sessionManagement(management -> management.sessionCreationPolicy(SessionCreationPolicy.STATELESS));
        // Send unauthenticated people a 403 response without redirecting to a failure page
        http.exceptionHandling(handling -> handling.authenticationEntryPoint(authenticationEntryPoint).accessDeniedPage(null));
        // Extract principal information from incoming certificates so that we can limit access to specific DNs
        AllowedCallersFilter allowedCallersFilter = getAllowedCallersFilter(securityProperties);
        http.addFilterBefore(allowedCallersFilter, X509AuthenticationFilter.class);
        // Allow JWT authentication
        http.addFilterAfter(jwtFilter, X509AuthenticationFilter.class);
        // Block users with denied-access role
        DeniedAccessRoleFilter deniedAccessRoleFilter = new DeniedAccessRoleFilter(securityProperties);
        http.addFilterAfter(deniedAccessRoleFilter, AbstractPreAuthenticatedProcessingFilter.class);
    }

    /**
     * Returns the authentication providers used to build this configuration's {@link AuthenticationManager}. Subclasses that add providers should call
     * {@code super.getAuthenticationProviders()} and append to the result.
     *
     * @return the authentication providers, in the order they should be consulted
     */
    protected List<AuthenticationProvider> getAuthenticationProviders() {
        List<AuthenticationProvider> providers = new ArrayList<>();
        providers.add(jwtAuthenticationProvider);
        return providers;
    }

    protected AllowedCallersFilter getAllowedCallersFilter(DatawaveSecurityProperties securityProperties) {
        return new AllowedCallersFilter(securityProperties, authenticationEntryPoint);
    }

    /**
     * Allows access to static resources without authentication.
     *
     * @return a {@link WebSecurityCustomizer} that ignores the common static resource locations
     */
    protected WebSecurityCustomizer staticResourcesWebSecurityCustomizer() {
        return web -> web.ignoring().requestMatchers(PathRequest.toStaticResources().atCommonLocations());
    }

    protected AuthenticationEntryPoint getAuthenticationEntryPoint() {
        return authenticationEntryPoint;
    }

}
