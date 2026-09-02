package datawave.security.evidence;

import java.util.Objects;

/**
 * Represents evidence consisting of a JSON web token.
 */
public class JWTEvidence extends DatawaveEvidence {

    /**
     * The JWT token.
     */
    private final String token;

    public JWTEvidence(String token) {
        this.token = token;
    }

    @Override
    public String getUsername() {
        return token;
    }

    /**
     * Return the JWT token.
     *
     * @return the token
     */
    public String getToken() {
        return token;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        JWTEvidence evidence = (JWTEvidence) o;
        return Objects.equals(token, evidence.token);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(token);
    }

    @Override
    public String toString() {
        // todo - Maybe have a system property to turn off obscuration for debugging?
        // The token is obscured here to prevent leaking it to logs/output.
        return "JWTEvidence{" + "token='" + (token == null ? null : "<obscured>") + '\'' + ", decodedPrincipal=" + decodedPrincipal + '}';
    }
}
