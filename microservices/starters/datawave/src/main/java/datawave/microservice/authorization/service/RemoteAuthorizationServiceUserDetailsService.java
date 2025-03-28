package datawave.microservice.authorization.service;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.codahale.metrics.annotation.Timed;

import datawave.microservice.authorization.preauth.ProxiedEntityPreauthPrincipal;
import datawave.microservice.authorization.preauth.ProxiedEntityX509Filter;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.authorization.user.DatawaveUserDetailsFactory;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.ProxiedEntityUtils;

/**
 * An {@link AuthenticationUserDetailsService} that retrieves user information from a remote authorization service for a set of proxied entity names, and
 * combines the results into a {@link DatawaveUserDetails}.
 * <p>
 * This service assumes that the caller principal and proxied entities all need to be combined into a proxy chain that is authenticated. The purpose of this
 * service is for a microservice that has not received a JWT header to call out to a remote authorization service to retrieve authentication information. In
 * production, it is likely better for the JWT to be retrieved by a load balancer or other API gateway and inject the JWT header before calling a service.
 * Therefore, this service is mostly useful for debugging and if there are problems with the suggested approach.
 */
@Service
@Profile(RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE)
public class RemoteAuthorizationServiceUserDetailsService implements AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> {
    public static final String ACTIVATION_PROFILE = "remoteauth";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final WebClient webClient;
    private final JWTTokenHandler jwtTokenHandler;
    private final DatawaveUserDetailsFactory userDetailsFactory;
    
    @Autowired
    public RemoteAuthorizationServiceUserDetailsService(WebClient.Builder webClientBuilder, JWTTokenHandler jwtTokenHandler,
                    DatawaveUserDetailsFactory userDetailsFactory,
                    @Value("${datawave.authorization.uri:https://authorization:8443/authorization/v1/authorize}") String authorizationUri) {
        this.webClient = webClientBuilder.baseUrl(authorizationUri).build();
        this.jwtTokenHandler = jwtTokenHandler;
        this.userDetailsFactory = userDetailsFactory;
    }
    
    @Override
    @Timed(name = "dw.remoteAuthorizationService.loadUserDetails", absolute = true)
    public UserDetails loadUserDetails(PreAuthenticatedAuthenticationToken token) throws UsernameNotFoundException {
        logger.debug("Authenticating {} via the authorization microservice", token);
        
        Object principalObj = token.getPrincipal();
        if (!(principalObj instanceof ProxiedEntityPreauthPrincipal)) {
            return null;
        }
        ProxiedEntityPreauthPrincipal principal = (ProxiedEntityPreauthPrincipal) principalObj;
        
        try {
            // @formatter:off
            String jwt = webClient.get()
                .header(ProxiedEntityX509Filter.ENTITIES_HEADER, buildDNChain(principal, SubjectIssuerDNPair::subjectDN))
                .header(ProxiedEntityX509Filter.ISSUERS_HEADER, buildDNChain(principal, SubjectIssuerDNPair::issuerDN))
                .retrieve()
                .bodyToMono(String.class)
                .block();
            // @formatter:on
            logger.debug("Authentication {} on remote authentication service with {} result", principal.getUsername(), (jwt == null) ? "null" : "non-null");
            if (jwt != null) {
                Collection<DatawaveUser> principals = jwtTokenHandler.createUsersFromToken(jwt);
                long createTime = principals.stream().map(DatawaveUser::getCreationTime).min(Long::compareTo).orElse(System.currentTimeMillis());
                return userDetailsFactory.create(principals, createTime);
            } else {
                throw new UsernameNotFoundException("No entities found for " + principal.getUsername());
            }
        } catch (Exception e) {
            logger.error("Failed performing lookup of {}: {}", principal.getUsername(), e);
            throw new UsernameNotFoundException(e.getMessage(), e);
        }
    }
    
    private String buildDNChain(ProxiedEntityPreauthPrincipal principal, Function<SubjectIssuerDNPair,String> dnFunc) {
        // @formatter:off
        return "<" +
            Stream.concat(principal.getProxiedEntities().stream(), Stream.of(principal.getCallerPrincipal()))
                .map(dnFunc)
                .map(ProxiedEntityUtils::buildProxiedDN)
                .collect(Collectors.joining("><"))
            + ">";
        // @formatter:on
    }
}
