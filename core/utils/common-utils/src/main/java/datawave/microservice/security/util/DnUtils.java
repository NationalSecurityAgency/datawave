package datawave.microservice.security.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.ProxiedEntityUtils;

public class DnUtils {
    
    private final Pattern subjectDnPattern;
    
    /** Parsed NPE OU identifiers */
    private final List<String> npeOuList;
    
    public DnUtils(Pattern subjectDnPattern, List<String> npeOuList) {
        this.subjectDnPattern = subjectDnPattern;
        this.npeOuList = npeOuList;
    }
    
    public static String[] splitProxiedDNs(String proxiedDNs, boolean allowDups) {
        return ProxiedEntityUtils.splitProxiedDNs(proxiedDNs, allowDups);
    }
    
    public static String[] splitProxiedSubjectIssuerDNs(String proxiedDNs) {
        return ProxiedEntityUtils.splitProxiedSubjectIssuerDNs(proxiedDNs);
    }
    
    public static String buildProxiedDN(String... dns) {
        return ProxiedEntityUtils.buildProxiedDN(dns);
    }
    
    public Collection<String> buildNormalizedDNList(String subjectDN, String issuerDN, String proxiedSubjectDNs, String proxiedIssuerDNs) {
        List<String> dnList = new ArrayList<>();
        if (proxiedSubjectDNs != null) {
            if (proxiedIssuerDNs == null)
                throw new IllegalArgumentException("If proxied subject DNs are supplied, then issuer DNs must be supplied as well.");
            String[] subjectDNarray = splitProxiedDNs(proxiedSubjectDNs, true);
            String[] issuerDNarray = splitProxiedDNs(proxiedIssuerDNs, true);
            if (subjectDNarray.length != issuerDNarray.length)
                throw new IllegalArgumentException("Subject and issuer DN lists do not have the same number of entries: " + Arrays.toString(subjectDNarray)
                                + " vs " + Arrays.toString(issuerDNarray));
            for (int i = 0; i < subjectDNarray.length; ++i) {
                subjectDNarray[i] = normalizeDN(subjectDNarray[i]);
                issuerDNarray[i] = normalizeDN(issuerDNarray[i]);
                dnList.add(subjectDNarray[i]);
                dnList.add(issuerDNarray[i]);
                if (issuerDNarray[i].equalsIgnoreCase(subjectDNarray[i]))
                    throw new IllegalArgumentException("Subject DN " + issuerDNarray[i] + " was passed as an issuer DN.");
                if (subjectDnPattern.matcher(issuerDNarray[i]).find())
                    throw new IllegalArgumentException("It appears that a subject DN (" + issuerDNarray[i] + ") was passed as an issuer DN.");
            }
        }
        subjectDN = normalizeDN(subjectDN);
        issuerDN = normalizeDN(issuerDN);
        dnList.add(subjectDN.replaceAll("(?<!\\\\)([<>])", "\\\\$1"));
        dnList.add(issuerDN.replaceAll("(?<!\\\\)([<>])", "\\\\$1"));
        return dnList;
    }
    
    public String buildNormalizedProxyDN(String subjectDN, String issuerDN, String proxiedSubjectDNs, String proxiedIssuerDNs) {
        StringBuilder sb = new StringBuilder();
        for (String escapedDN : buildNormalizedDNList(subjectDN, issuerDN, proxiedSubjectDNs, proxiedIssuerDNs)) {
            if (sb.length() == 0)
                sb.append(escapedDN);
            else
                sb.append('<').append(escapedDN).append('>');
        }
        return sb.toString();
    }
    
    public static String buildNormalizedProxyDN(List<SubjectIssuerDNPair> dns) {
        StringBuilder sb = new StringBuilder();
        dns.stream().forEach(dn -> {
            if (sb.length() == 0) {
                sb.append(normalizeDN(dn.subjectDN()));
            } else {
                sb.append('<').append(normalizeDN(dn.subjectDN())).append('>');
            }
            sb.append('<').append(normalizeDN(dn.issuerDN())).append('>');
        });
        return sb.toString();
    }
    
    public static String getCommonName(String dn) {
        return ProxiedEntityUtils.getCommonName(dn);
    }
    
    public static String[] getOrganizationalUnits(String dn) {
        return ProxiedEntityUtils.getOrganizationalUnits(dn);
    }
    
    public static String getShortName(String dn) {
        return ProxiedEntityUtils.getShortName(dn);
    }
    
    public boolean isServerDN(String dn) {
        return isNPE(dn);
    }
    
    public String getUserDN(String[] dns) {
        return getUserDN(dns, false);
    }
    
    public String getUserDN(String[] dns, boolean issuerDNs) {
        if (issuerDNs && (dns.length % 2) != 0)
            throw new IllegalArgumentException("DNs array is not a subject/issuer DN list: " + Arrays.toString(dns));
        
        for (int i = 0; i < dns.length; i += (issuerDNs) ? 2 : 1) {
            String dn = dns[i];
            if (!isServerDN(dn))
                return dn;
        }
        return null;
    }
    
    public String[] getComponents(String dn, String componentName) {
        return ProxiedEntityUtils.getComponents(dn, componentName);
    }
    
    public static String normalizeDN(String userName) {
        return ProxiedEntityUtils.normalizeDN(userName);
    }
    
    private boolean isNPE(String dn) {
        String[] ouList = ProxiedEntityUtils.getOrganizationalUnits(dn);
        for (String ou : ouList) {
            if (npeOuList.contains(ou.toUpperCase())) {
                return true;
            }
        }
        return false;
    }
}
