package datawave.security.evidence;

import java.security.Principal;

import org.wildfly.security.evidence.Evidence;

/**
 * Base implementation of {@link Evidence} that is capable of storing a decoded principal.
 */
public abstract class DatawaveEvidence implements Evidence {

    // The decoded principal.
    protected Principal decodedPrincipal;

    /**
     * Return the username associated with this evidence.
     *
     * @return the username
     */
    public abstract String getUsername();

    /**
     * Return the default principal (i.e., the decoded principal).
     *
     * @return the default principal, possibly null
     */
    @Override
    public Principal getDefaultPrincipal() {
        return getDecodedPrincipal();
    }

    /**
     * Return the decoded principal
     *
     * @return the decoded principal, possibly null
     */
    @Override
    public Principal getDecodedPrincipal() {
        return decodedPrincipal;
    }

    /**
     * Set the decoded principal
     */
    @Override
    public void setDecodedPrincipal(Principal principal) {
        this.decodedPrincipal = principal;
    }
}
