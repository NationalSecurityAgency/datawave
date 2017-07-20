package datawave.security.authorization;

import datawave.security.util.DnUtils;

/**
 * A simple pair containing a subject and (optional) issuer DN. The supplied DN values are normalized into a lower-case form with the CN portion first.
 */
public class SubjectIssuerDNPair {
    private final String subjectDN;
    private final String issuerDN;
    
    public static SubjectIssuerDNPair of(String subjectDN) {
        return new SubjectIssuerDNPair(subjectDN, null);
    }
    
    public static SubjectIssuerDNPair of(String subjectDN, String issuerDN) {
        return new SubjectIssuerDNPair(subjectDN, issuerDN);
    }
    
    protected SubjectIssuerDNPair(String subjectDN, String issuerDN) {
        this.subjectDN = DnUtils.normalizeDN(subjectDN);
        if (issuerDN != null) {
            this.issuerDN = DnUtils.normalizeDN(issuerDN);
        } else {
            this.issuerDN = null;
        }
    }
    
    public String subjectDN() {
        return subjectDN;
    }
    
    public String issuerDN() {
        return issuerDN;
    }
    
    @Override
    public String toString() {
        return issuerDN == null ? subjectDN + "<>" : DnUtils.buildProxiedDN(subjectDN, issuerDN);
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
