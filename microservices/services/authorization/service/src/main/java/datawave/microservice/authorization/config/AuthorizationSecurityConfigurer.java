package datawave.microservice.authorization.config;

import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationProvider;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

import datawave.microservice.authorization.jwt.JWTAuthenticationFilter;
import datawave.microservice.authorization.jwt.JWTAuthenticationProvider;
import datawave.microservice.authorization.preauth.AuthorizationProxiedEntityX509Filter;
import datawave.microservice.config.security.AllowedCallersFilter;
import datawave.microservice.config.security.JWTSecurityConfigurer;

/**
 * <p>
 * Configures security for the authorization microservice.
 * <p>
 * <u>AuthorizationOperations</u>:
 * <p>
 * Only allowed callers can call these endpoints. A caller can supply a signed JSON Web Token (JWT) containing authorization credentials or can authenticate by
 * using a client certificate (https) or via X-SSL-clientcert-subject/X-SSL-clientcert-issuer http headers (http) to establish that they are a trusted caller.
 * <p>
 * Callers using a client certificate or trusted headers usually supply X-ProxiedEntitiesChain/X-ProxiedIssuersChain http headers because it is assumed that
 * another service is authenticating on behalf of a user, and therefore <em>only</em> the X-ProxiedEntitiesChain/X-ProxiedIssuersChain values are used to
 * calculate the authenticated principal and the caller's information is only used to determine if the caller is trusted. This behavior can be changed if
 * {@link DatawaveSecurityProperties#isProxiedEntitiesRequired()} is set to false. In that case, if the caller supplies no
 * X-ProxiedEntitiesChain/X-ProxiedIssuersChain header, then the caller's information is copied into the X-ProxiedEntitiesChain/X-ProxiedIssuersChain header so
 * that the caller is proxying for itself. This is a convenience used for testing when one would like a user to be able to directly access
 * AuthorizationOperations (e.g., from a web browser).
 * <p>
 * <u>OAuthOperations</u>:
 * <p>
 * Security is maintained through a list of registered clients (client_id, client_secret), redirects, and time limitations which allow any user to call
 * oauth/authorize as part of the OAuth2 code flow when using a registered client service. This flow made it necessary to bypass the allowedCallers check that
 * is used for the AuthorizationOperations.
 * <p>
 * <u>Security Filter Chain</u>:
 * <p>
 * AuthorizationAllowedCallersFilter (extends AllowedCallersFilter) is executed before the spring X509AuthenticationFilter and will check for allowedCallers if
 * there is a client certificate. The allowedCallers check is bypassed for paths with an oauth prefix.
 * <p>
 * JWTAuthenticationFilter checks if there is a JWT Bearer token in the Authorization http header. If a valid JWT is found, then that identity is used.
 * <p>
 * AuthorizationProxiedEntityX509Filter (extends ProxiedEntityX509Filter) checks if authorization has already happened (JWT). If not, then the configured
 * UserDetailsService is used. For the authorization microservice, this is the ProxiedEntityUserDetailsService. AuthorizationProxiedEntityX509Filter overrides
 * getPreAuthenticatedPrincipal() to call super.getPreAuthenticatedPrincipal() for a ProxiedEntityPreauthPrincipal and then if non-null creates a subclass
 * AuthorizationProxiedEntityPreauthPrincipal which contains the HttpServletRequest. This allows ProxiedEntityUserDetailsService to call
 * AuthorizationAllowedCallersFilter.enforceAllowedCallersForRequest() to determine if allowedCallers should enforced for that request.
 */
@Order(SecurityProperties.BASIC_AUTH_ORDER - 3)
@Configuration
@EnableCaching
public class AuthorizationSecurityConfigurer extends JWTSecurityConfigurer {
    private final DatawaveSecurityProperties securityProperties;
    private final AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> authenticationUserDetailsService;
    
    public AuthorizationSecurityConfigurer(DatawaveSecurityProperties securityProperties,
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
        // X-ProxiedEntitiesChain/X-ProxiedIssuersChain headers that are supplied by trusted callers.
        AuthorizationProxiedEntityX509Filter proxiedX509Filter = new AuthorizationProxiedEntityX509Filter(securityProperties.isUseTrustedSubjectHeaders(),
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
    
    @Override
    protected AllowedCallersFilter getAllowedCallersFilter(DatawaveSecurityProperties securityProperties) {
        return new AuthorizationAllowedCallersFilter(securityProperties, getAuthenticationEntryPoint());
    }
}
