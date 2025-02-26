package datawave.microservice.authorization.jwt;

import java.util.Collection;
import java.util.Collections;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

/**
 * A holder for an unparsed, unvalidated JWT authorization token.
 */
public class JWTPreauthToken implements Authentication {
    private static final long serialVersionUID = 1L;
    
    private final String jwtTokenString;
    
    public JWTPreauthToken(String jwtTokenString) {
        this.jwtTokenString = jwtTokenString;
    }
    
    @Override
    public String getCredentials() {
        return jwtTokenString;
    }
    
    @Override
    public Object getPrincipal() {
        return getName();
    }
    
    @Override
    public String getName() {
        return "Unauthenticated JWT";
    }
    
    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return Collections.emptyList();
    }
    
    @Override
    public Object getDetails() {
        return null;
    }
    
    @Override
    public boolean isAuthenticated() {
        return false;
    }
    
    @Override
    public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException {
        if (isAuthenticated) {
            throw new IllegalArgumentException("Cannot force a " + getClass().getSimpleName() + " to be authenticated.");
        }
    }
}
