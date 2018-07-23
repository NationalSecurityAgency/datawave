package datawave.security.authorization;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import datawave.security.util.ProxiedEntityUtils;

import java.io.Serializable;

/**
 * A simple pair containing a subject and (optional) issuer DN. The supplied DN values are normalized into a lower-case form with the CN portion first.
 */
public class SubjectIssuerDNPair implements Serializable {
    private final String subjectDN;
    private final String issuerDN;
    
    public static SubjectIssuerDNPair of(String subjectDN) {
        return new SubjectIssuerDNPair(subjectDN, null);
    }
    
    @JsonCreator
    public static SubjectIssuerDNPair of(@JsonProperty("subjectDN") String subjectDN, @JsonProperty("issuerDN") String issuerDN) {
        return new SubjectIssuerDNPair(subjectDN, issuerDN);
    }
    
    public static SubjectIssuerDNPair parse(String dn) {
        String[] dns = ProxiedEntityUtils.splitProxiedSubjectIssuerDNs(dn);
        if (dns.length != 2)
            throw new IllegalArgumentException(dn + " must contain a single subject and issuer DN");
        return new SubjectIssuerDNPair(dns[0], dns[1]);
    }
    
    protected SubjectIssuerDNPair(String subjectDN, String issuerDN) {
        this.subjectDN = ProxiedEntityUtils.normalizeDN(subjectDN);
        if (issuerDN != null) {
            this.issuerDN = ProxiedEntityUtils.normalizeDN(issuerDN);
        } else {
            this.issuerDN = null;
        }
    }
    
    @JsonGetter
    public String subjectDN() {
        return subjectDN;
    }
    
    @JsonGetter
    public String issuerDN() {
        return issuerDN;
    }
    
    @Override
    public String toString() {
        return issuerDN == null ? subjectDN + "<>" : ProxiedEntityUtils.buildProxiedDN(subjectDN, issuerDN);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        
        SubjectIssuerDNPair that = (SubjectIssuerDNPair) o;
        
        if (!subjectDN.equals(that.subjectDN))
            return false;
        return issuerDN != null ? issuerDN.equals(that.issuerDN) : that.issuerDN == null;
    }
    
    @Override
    public int hashCode() {
        int result = subjectDN.hashCode();
        result = 31 * result + (issuerDN != null ? issuerDN.hashCode() : 0);
        return result;
    }
}
