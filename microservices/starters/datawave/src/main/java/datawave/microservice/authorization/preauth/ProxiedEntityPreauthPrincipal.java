package datawave.microservice.authorization.preauth;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import datawave.security.authorization.SubjectIssuerDNPair;

/**
 * A principal object representing a pre-authenticated caller principal (e.g., authenticated with X.509 certificates) that is acting as a proxy for a number of
 * other entities.
 */
public class ProxiedEntityPreauthPrincipal implements Serializable {
    private static final long serialVersionUID = 2806027352312288765L;
    
    private final SubjectIssuerDNPair callerPrincipal;
    private final Collection<SubjectIssuerDNPair> proxiedEntities;
    private final String username;
    
    public ProxiedEntityPreauthPrincipal(SubjectIssuerDNPair callerPrincipal, Collection<SubjectIssuerDNPair> proxiedEntities) {
        this.callerPrincipal = callerPrincipal;
        this.proxiedEntities = proxiedEntities != null ? proxiedEntities : Collections.emptyList();
        this.username = proxiedEntities == null ? null : proxiedEntities.stream().map(SubjectIssuerDNPair::toString).collect(Collectors.joining(" -> "));
    }
    
    public SubjectIssuerDNPair getCallerPrincipal() {
        return callerPrincipal;
    }
    
    public Collection<SubjectIssuerDNPair> getProxiedEntities() {
        return proxiedEntities;
    }
    
    public String getUsername() {
        return username;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        
        ProxiedEntityPreauthPrincipal that = (ProxiedEntityPreauthPrincipal) o;
        
        if (!callerPrincipal.equals(that.callerPrincipal))
            return false;
        return proxiedEntities.equals(that.proxiedEntities);
    }
    
    @Override
    public int hashCode() {
        int result = callerPrincipal.hashCode();
        result = 31 * result + proxiedEntities.hashCode();
        return result;
    }
    
    @Override
    public String toString() {
        // @formatter:off
        return "ProxiedEntityPreauthPrincipal{" +
                "callerPrincipal=" + callerPrincipal +
                ", proxiedEntities=" + proxiedEntities +
                '}';
        // @formatter:on
    }
}
