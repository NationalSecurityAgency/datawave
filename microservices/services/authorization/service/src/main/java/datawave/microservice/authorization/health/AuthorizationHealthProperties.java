package datawave.microservice.authorization.health;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "datawave.authorization.health")
public class AuthorizationHealthProperties {

    /**
     * Whether the authorization health check is enabled.
     */
    private boolean enabled = true;

    /**
     * The subject DN of a dummy user to look up during the health check.
     */
    private String subjectDn;

    /**
     * The issuer DN of the dummy user to look up during the health check. Optional.
     */
    private String issuerDn;
}
