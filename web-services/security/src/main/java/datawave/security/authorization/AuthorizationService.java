package datawave.security.authorization;

import com.codahale.metrics.annotation.Timed;

/**
 * The interface for retrieving a user's roles.
 */
public interface AuthorizationService {
    
    /**
     * Get the roles for {@code userDN}/{@code issuerDN} pair in the project {@code projectName}.
     * 
     * @param projectName
     *            the name of the project or group to query
     * @param userDN
     *            the DN of the user whose roles are to be retrieved
     * @param issuerDN
     *            the DN of the certificate issuer for the certificate presented by {@code userDN}
     * @return the list of roles for {@code userDN} on {@code projectName}, or {@code null}
     */
    @Timed(name = "dw.auth.getRoles", absolute = true)
    String[] getRoles(String projectName, String userDN, String issuerDN);
    
}
