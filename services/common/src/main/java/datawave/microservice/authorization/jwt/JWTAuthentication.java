package datawave.microservice.authorization.jwt;

import datawave.microservice.authorization.user.ProxiedUserDetails;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents the {@link Authentication} of a request that is secured with a JSON Web Token (JWT) found in the Authorization header.
 */
public class JWTAuthentication implements Authentication {
    private static final long serialVersionUID = 1L;
    
    private final ProxiedUserDetails userDetails;
    private final List<GrantedAuthority> authorities;
    private boolean authenticated;
    
    public JWTAuthentication(ProxiedUserDetails userDetails) {
        this.userDetails = userDetails;
        authorities = userDetails.getPrimaryUser().getRoles().stream().map(SimpleGrantedAuthority::new).collect(Collectors.toList());
        authenticated = true;
    }
    
    @Override
    public String getName() {
        return userDetails.getUsername();
    }
    
    @Override
    public Object getPrincipal() {
        return userDetails;
    }
    
    @Override
    public Object getCredentials() {
        return userDetails.getPassword();
    }
    
    @Override
    public Object getDetails() {
        return null;
    }
    
    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return authorities;
    }
    
    @Override
    public boolean isAuthenticated() {
        return authenticated;
    }
    
    @Override
    public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException {
        authenticated = isAuthenticated;
    }
}
