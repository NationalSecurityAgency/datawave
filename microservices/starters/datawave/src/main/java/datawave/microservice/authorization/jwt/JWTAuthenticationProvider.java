package datawave.microservice.authorization.jwt;

import java.util.Collection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.stereotype.Component;

import datawave.microservice.authorization.jwt.exception.InvalidSignatureException;
import datawave.microservice.authorization.jwt.exception.InvalidTokenException;
import datawave.microservice.authorization.jwt.exception.TokenExpiredException;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.authorization.user.DatawaveUserDetailsFactory;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.JWTTokenHandler;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureException;
import io.jsonwebtoken.UnsupportedJwtException;

/**
 * An {@link AuthenticationProvider} that accepts {@link JWTPreauthToken}s and attempts to convert the included JWT token back into a
 * {@link DatawaveUserDetails} .
 */
@Component
@ConditionalOnWebApplication
public class JWTAuthenticationProvider implements AuthenticationProvider {
    private final JWTTokenHandler tokenHandler;
    private final DatawaveUserDetailsFactory userDetailsFactory;
    
    @Autowired
    public JWTAuthenticationProvider(JWTTokenHandler tokenHandler, DatawaveUserDetailsFactory userDetailsFactory) {
        this.tokenHandler = tokenHandler;
        this.userDetailsFactory = userDetailsFactory;
    }
    
    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        if (authentication instanceof JWTPreauthToken) {
            JWTPreauthToken jwtPreauthToken = (JWTPreauthToken) authentication;
            try {
                Collection<? extends DatawaveUser> users = tokenHandler.createUsersFromToken(jwtPreauthToken.getCredentials());
                long minCreateTime = users.stream().map(DatawaveUser::getCreationTime).min(Long::compareTo).orElse(System.currentTimeMillis());
                DatawaveUserDetails datawaveUserDetails = userDetailsFactory.create(users, minCreateTime);
                return new JWTAuthentication(datawaveUserDetails);
            } catch (UnsupportedJwtException | MalformedJwtException | IllegalArgumentException e) {
                throw new InvalidTokenException("JWT is not valid.", e);
            } catch (SignatureException e) {
                throw new InvalidSignatureException("JWT signature validation failed", e);
            } catch (ExpiredJwtException e) {
                throw new TokenExpiredException(e.getMessage(), e);
            }
        }
        return null;
    }
    
    @Override
    public boolean supports(Class<?> authentication) {
        return JWTPreauthToken.class.isAssignableFrom(authentication);
    }
}
