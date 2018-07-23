package datawave.security.authorization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.CompressionCodecs;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.DefaultJwtBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Key;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Converts between a String encoded JSON Web Token and a collection of {@link DatawaveUser}s.
 */
public class JWTTokenHandler {
    private static final String PRINCIPALS_CLAIM = "principals";
    
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private final String issuer;
    private final Key signingKey;
    private final long jwtTtl;
    private final ObjectMapper objectMapper;
    
    public JWTTokenHandler(Certificate cert, Key signingKey, long jwtTtl, TimeUnit jwtTtUnit, ObjectMapper objectMapper) {
        this.signingKey = signingKey;
        this.jwtTtl = jwtTtUnit.toMillis(jwtTtl);
        this.objectMapper = objectMapper;
        if (cert instanceof X509Certificate) {
            issuer = ((X509Certificate) cert).getSubjectDN().getName();
        } else {
            issuer = "DATAWAVE";
        }
    }
    
    public String createTokenFromUsers(String username, Collection<? extends DatawaveUser> users) {
        long minCreationTime = users.stream().map(DatawaveUser::getCreationTime).min(Long::compareTo).orElse(System.currentTimeMillis());
        Date expirationDate = new Date(minCreationTime + jwtTtl);
        logger.trace("Creating new JWT to expire at {} for users {}", expirationDate, users);
        // @formatter:off
        return new CustomJWTBuilder(objectMapper)
                .setSubject(username)
                .setAudience("DATAWAVE")
                .setIssuer(issuer)
                .setExpiration(expirationDate)
                .claim(PRINCIPALS_CLAIM, users)
                .signWith(SignatureAlgorithm.RS512, signingKey)
                .compressWith(CompressionCodecs.GZIP)
                .compact();
        // @formatter:on
    }
    
    public Collection<DatawaveUser> createUsersFromToken(String token) {
        logger.trace("Attempting to parse JWT {}", token);
        Jws<Claims> claimsJws = Jwts.parser().setSigningKey(signingKey).parseClaimsJws(token);
        Claims claims = claimsJws.getBody();
        logger.trace("Resulting claims: {}", claims);
        List<?> principalsClaim = claims.get(PRINCIPALS_CLAIM, List.class);
        if (principalsClaim == null || principalsClaim.isEmpty()) {
            throw new IllegalArgumentException("JWT for " + claims.getSubject() + " does not contain any proxied principals.");
        }
        return principalsClaim.stream().map(obj -> objectMapper.convertValue(obj, DatawaveUser.class)).collect(Collectors.toList());
    }
    
    private class CustomJWTBuilder extends DefaultJwtBuilder {
        private final ObjectMapper objectMapper;
        
        private CustomJWTBuilder(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }
        
        @Override
        protected byte[] toJson(Object object) throws JsonProcessingException {
            return objectMapper.writeValueAsBytes(object);
        }
    }
}
