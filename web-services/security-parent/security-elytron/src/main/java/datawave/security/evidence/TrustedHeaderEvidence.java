package datawave.security.evidence;

import java.util.List;
import java.util.StringJoiner;

import datawave.security.authorization.SubjectIssuerDNPair;

/**
 * Represents evidence consisting of trusted headers.
 */
public class TrustedHeaderEvidence extends DatawaveEvidence {

    /**
     * The username.
     */
    private final String username;

    /**
     * The set of entities consisting of the user entity and any proxied entities.
     */
    private final List<SubjectIssuerDNPair> entities;

    public TrustedHeaderEvidence(String username, List<SubjectIssuerDNPair> entities) {
        this.username = username;
        this.entities = List.copyOf(entities);
    }

    @Override
    public String getUsername() {
        return username;
    }

    public List<SubjectIssuerDNPair> getEntities() {
        return entities;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TrustedHeaderEvidence.class.getSimpleName() + "[", "]").add("username='" + username + "'").add("entities=" + entities)
                        .add("decodedPrincipal=" + decodedPrincipal).toString();
    }
}
