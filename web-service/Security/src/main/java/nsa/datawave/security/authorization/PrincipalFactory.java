package nsa.datawave.security.authorization;

import java.io.Serializable;

import nsa.datawave.security.authorization.DatawavePrincipal;

import org.jboss.logging.Logger;

public interface PrincipalFactory extends Serializable {
    
    public abstract DatawavePrincipal createPrincipal(String userName, String[] roles);
    
    public abstract void mergePrincipals(DatawavePrincipal target, DatawavePrincipal additional);
    
    /**
     * Convert the supplied roles from the authorization service into authorization strings suitable for passing to Accumulo.
     * 
     * @param roles
     *            the authorization service supplied roles to convert to Accumulo authorizations
     * @return Accumulo authorization strings converted from {@code roles}
     */
    public String[] toAccumuloAuthorizations(String[] roles);
    
    /**
     * Maps {@code originalRoles} into a new set of authorization service roles. The intent of this method is to perform translation on the authorization
     * service's responses to add domain-specific values that may not be supplied natively by the authorization service.
     * 
     * @param subjectDN
     *            the DN of the subject to whom {@code originalRoles} belong
     * @param issuerDN
     *            the issuer of {@code subjectDN}'s certificate
     * @param originalRoles
     *            the roles, returned from the authorization service, to remap into a new set of roles.
     * @return the remapped authorization service roles
     */
    public String[] remapRoles(String userName, String[] originalRoles);
    
}
