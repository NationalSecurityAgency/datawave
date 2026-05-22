package datawave.microservice.authorization.health;

import java.util.Collection;
import java.util.Collections;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;

/**
 * Health indicator for the authorization service. Performs a user lookup using a configurable dummy DN to verify the authorization pipeline is functional.
 *
 * <p>
 * Configure via application properties:
 *
 * <pre>
 * datawave.authorization.health.enabled=true
 * datawave.authorization.health.subject-dn=cn=healthcheck, ou=testing, o=example, c=us
 * datawave.authorization.health.issuer-dn=cn=ca, ou=testing, o=example, c=us
 * </pre>
 */
@Component
@ConditionalOnProperty(name = "datawave.authorization.health.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(AuthorizationHealthProperties.class)
public class AuthorizationHealthChecker implements HealthIndicator {

    private final CachedDatawaveUserService cachedDatawaveUserService;
    private final AuthorizationHealthProperties healthProperties;

    public AuthorizationHealthChecker(CachedDatawaveUserService cachedDatawaveUserService, AuthorizationHealthProperties healthProperties) {
        this.cachedDatawaveUserService = cachedDatawaveUserService;
        this.healthProperties = healthProperties;
    }

    @Override
    public Health health() {
        Health.Builder builder = new Health.Builder();

        String subjectDn = healthProperties.getSubjectDn();
        if (subjectDn == null || subjectDn.isBlank()) {
            // No dummy user configured — just verify the service bean is wired correctly
            builder.up();
            builder.withDetail("message", "No health check DN configured. Set datawave.authorization.health.subject-dn to enable user lookup validation.");
            return builder.build();
        }

        String issuerDn = healthProperties.getIssuerDn();
        SubjectIssuerDNPair dn = (issuerDn != null && !issuerDn.isBlank()) ? SubjectIssuerDNPair.of(subjectDn, issuerDn) : SubjectIssuerDNPair.of(subjectDn);

        builder.withDetail("subjectDn", dn.subjectDN());
        if (dn.issuerDN() != null) {
            builder.withDetail("issuerDn", dn.issuerDN());
        }

        try {
            Collection<DatawaveUser> users = cachedDatawaveUserService.lookup(Collections.singleton(dn));

            if (users == null || users.isEmpty()) {
                builder.down();
                builder.withDetail("error", "User lookup returned no results for the configured health check DN");
            } else {
                DatawaveUser user = users.iterator().next();
                builder.up();
                builder.withDetail("userType", user.getUserType().name());
                builder.withDetail("roles", user.getRoles());
                builder.withDetail("auths", user.getAuths());
            }
        } catch (Exception e) {
            builder.down(e);
        }

        return builder.build();
    }
}
