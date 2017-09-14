package datawave.microservice.authorization.jwt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import datawave.microservice.authorization.config.DatawaveSecurityProperties;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.DatawaveUser;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.CompressionCodecs;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.DefaultJwtBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.context.embedded.Ssl;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Converts between a String encoded JSON Web Token and a {@link ProxiedUserDetails}.
 */
@Component
public class JWTTokenHandler {
    private static final String PRINCIPALS_CLAIM = "principals";
    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.registerModule(new GuavaModule());
    }
    
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private final String issuer;
    private final Key signingKey;
    
    private final DatawaveSecurityProperties securityProperties;
    
    public JWTTokenHandler(ServerProperties serverProperties, DatawaveSecurityProperties securityProperties) {
        this.securityProperties = securityProperties;
        try {
            Ssl ssl = serverProperties.getSsl();
            String keyStoreType = ssl.getKeyStoreType();
            KeyStore keyStore = KeyStore.getInstance(keyStoreType == null ? "JKS" : keyStoreType);
            char[] keyPassword = ssl.getKeyPassword() != null ? ssl.getKeyPassword().toCharArray() : ssl.getKeyStorePassword().toCharArray();
            keyStore.load(ResourceUtils.getURL(ssl.getKeyStore()).openStream(), keyPassword);
            String alias = keyStore.aliases().nextElement();
            signingKey = keyStore.getKey(alias, ssl.getKeyStorePassword().toCharArray());
            Certificate cert = keyStore.getCertificate(alias);
            if (cert instanceof X509Certificate) {
                issuer = ((X509Certificate) cert).getSubjectDN().getName();
            } else {
                issuer = "DATAWAVE";
            }
        } catch (Exception e) {
            throw new IllegalStateException("Invalid SSL configuration.", e);
        }
    }
    
    public String createTokenFromUser(ProxiedUserDetails principal) {
        long minCreationTime = principal.getProxiedUsers().stream().map(DatawaveUser::getCreationTime).min(Long::compareTo).orElse(System.currentTimeMillis());
        Date expirationDate = new Date(minCreationTime + TimeUnit.SECONDS.toMillis(securityProperties.getJwt().getTtl()));
        logger.trace("Creating new JWT to expire at {} for user {}", expirationDate, principal);
        // @formatter:off
        return new CustomJWTBuilder()
                .setSubject(principal.getUsername())
                .setAudience("DATAWAVE")
                .setIssuer(issuer)
                .setExpiration(expirationDate)
                .claim(PRINCIPALS_CLAIM, principal.getProxiedUsers())
                .signWith(SignatureAlgorithm.RS512, signingKey)
                .compressWith(CompressionCodecs.GZIP)
                .compact();
        // @formatter:on
    }
    
    public ProxiedUserDetails createPrincipalFromToken(String token) {
        logger.trace("Attempting to parse JWT {}", token);
        Jws<Claims> claimsJws = Jwts.parser().setSigningKey(signingKey).parseClaimsJws(token);
        Claims claims = claimsJws.getBody();
        logger.trace("Resulting claims: {}", claims);
        List<?> principalsClaim = claims.get(PRINCIPALS_CLAIM, List.class);
        if (principalsClaim == null || principalsClaim.isEmpty()) {
            throw new IllegalArgumentException("JWT for " + claims.getSubject() + " does not contain any proxied principals.");
        }
        List<DatawaveUser> proxiedUsers = principalsClaim.stream().map(obj -> OBJECT_MAPPER.convertValue(obj, DatawaveUser.class)).collect(Collectors.toList());
        long minCreateTime = proxiedUsers.stream().map(DatawaveUser::getCreationTime).min(Long::compareTo).orElse(System.currentTimeMillis());
        return new ProxiedUserDetails(proxiedUsers, minCreateTime);
    }
    
    private class CustomJWTBuilder extends DefaultJwtBuilder {
        @Override
        protected byte[] toJson(Object object) throws JsonProcessingException {
            return OBJECT_MAPPER.writeValueAsBytes(object);
        }
    }
}
