package datawave.microservice.authorization.userdetails;

import datawave.microservice.authorization.config.DatawaveSecurityProperties;
import datawave.microservice.authorization.preauth.ProxiedEntityPreauthPrincipal;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * An {@link AuthenticationUserDetailsService} that retrieves user information from a {@link DatawaveUserService} for a set of proxied entity names, and
 * combines the results into a {@link ProxiedUserDetails}.
 * <p>
 * Note that for authentication purposes, it is assumed that another service is authenticating on behalf of a user, and therefore <em>only</em> the
 * X-ProxiedEntitiesChain/X-ProxiedIssuersChain values are used to calculate the authenticated principal and the incoming certificate is only used to determine
 * if the caller is trusted. This behavior can be change if {@link DatawaveSecurityProperties#isProxiedEntitiesRequired()} is set to false. In that case, if the
 * caller supplies no X-ProxiedEntitiesChain/X-ProxiedIssuersChain header, the incoming certificate subject and issuer are copied into the
 * X-ProxiedEntitiesChain/X-ProxiedIssuersChain header so that the caller is proxying for itself. This is a convenience used for testing when one would like a
 * user to be able to directly access the authorization service (e.g., from a web browser).
 */
@Service
public class ProxiedEntityUserDetailsService implements AuthenticationUserDetailsService<PreAuthenticatedAuthenticationToken> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final DatawaveUserService datawaveUserService;
    private final DatawaveSecurityProperties securityProperties;
    
    @Autowired
    public ProxiedEntityUserDetailsService(DatawaveUserService datawaveUserService, DatawaveSecurityProperties securityProperties) {
        this.datawaveUserService = datawaveUserService;
        this.securityProperties = securityProperties;
    }
    
    @Override
    public UserDetails loadUserDetails(PreAuthenticatedAuthenticationToken token) throws UsernameNotFoundException {
        logger.debug("Authenticating {}", token);
        
        Object principalObj = token.getPrincipal();
        if (!(principalObj instanceof ProxiedEntityPreauthPrincipal)) {
            return null;
        }
        ProxiedEntityPreauthPrincipal principal = (ProxiedEntityPreauthPrincipal) principalObj;
        
        if (securityProperties.isEnforceAllowedCallers()) {
            final Collection<String> allowedCallers = securityProperties.getAllowedCallers();
            if (!allowedCallers.contains(principal.getCallerPrincipal().toString())) {
                logger.warn("Not allowing {} to talk since it is not in the allowed list of users {}", principalObj, allowedCallers);
                throw new BadCredentialsException(principalObj + " is not allowed to call.");
            }
        } else {
            logger.trace("Allowing {} since we're not enforcing allowed callers.", principalObj);
        }
        
        try {
            List<DatawaveUser> principals = new ArrayList<>(datawaveUserService.lookup(principal.getProxiedEntities()));
            long createTime = principals.stream().map(DatawaveUser::getCreationTime).min(Long::compareTo).orElse(System.currentTimeMillis());
            return new ProxiedUserDetails(principals, createTime);
        } catch (AuthorizationException e) {
            logger.error("Failed performing lookup of {}: {}", principal.getUsername(), e);
            throw new UsernameNotFoundException(e.getMessage(), e);
        }
    }
}
