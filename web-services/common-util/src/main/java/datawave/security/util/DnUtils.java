package datawave.security.util;

import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.user.AuthorizationsListBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class DnUtils {
    
    /** Config for injecting NPE OU identifiers */
    public static final String PROPS_RESOURCE = "dnutils.properties";
    
    private static final Properties PROPS = new Properties();
    
    public static final String SUBJECT_DN_PATTERN_PROPERTY = "subject.dn.pattern";
    
    private static final Pattern SUBJECT_DN_PATTERN;
    
    private static final Logger log = LoggerFactory.getLogger(DnUtils.class);
    
    static {
        InputStream in = null;
        try {
            in = DnUtils.class.getClassLoader().getResourceAsStream(PROPS_RESOURCE);
            PROPS.load(in);
        } catch (Throwable t) {
            log.error(PROPS_RESOURCE + " could not be loaded!", t);
            throw new RuntimeException(t);
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException e) {
                    log.warn("Failed to close input stream", e);
                }
            }
        }
        String subjectDnPattern = System.getProperty(SUBJECT_DN_PATTERN_PROPERTY, PROPS.getProperty(SUBJECT_DN_PATTERN_PROPERTY));
        try {
            if (null == subjectDnPattern || subjectDnPattern.isEmpty()) {
                throw new IllegalStateException(SUBJECT_DN_PATTERN_PROPERTY + " property value cannot be null");
            }
            SUBJECT_DN_PATTERN = Pattern.compile(subjectDnPattern, Pattern.CASE_INSENSITIVE);
        } catch (Throwable t) {
            log.error(SUBJECT_DN_PATTERN_PROPERTY + " = '" + subjectDnPattern + "' could not be compiled", t);
            throw new RuntimeException(t);
        }
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
    
    public static Collection<String> buildNormalizedDNList(String subjectDN, String issuerDN, String proxiedSubjectDNs, String proxiedIssuerDNs) {
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
                if (SUBJECT_DN_PATTERN.matcher(issuerDNarray[i]).find())
                    throw new IllegalArgumentException("It appears that a subject DN (" + issuerDNarray[i] + ") was passed as an issuer DN.");
            }
        }
        subjectDN = normalizeDN(subjectDN);
        issuerDN = normalizeDN(issuerDN);
        dnList.add(subjectDN.replaceAll("(?<!\\\\)([<>])", "\\\\$1"));
        dnList.add(issuerDN.replaceAll("(?<!\\\\)([<>])", "\\\\$1"));
        return dnList;
    }
    
    public static String buildNormalizedProxyDN(String subjectDN, String issuerDN, String proxiedSubjectDNs, String proxiedIssuerDNs) {
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
    
    public static boolean isServerDN(String dn) {
        return NpeUtils.isNPE(dn);
    }
    
    public static String getUserDN(String[] dns) {
        return getUserDN(dns, false);
    }
    
    public static String getUserDN(String[] dns, boolean issuerDNs) {
        if (issuerDNs && (dns.length % 2) != 0)
            throw new IllegalArgumentException("DNs array is not a subject/issuer DN list: " + Arrays.toString(dns));
        
        for (int i = 0; i < dns.length; i += (issuerDNs) ? 2 : 1) {
            String dn = dns[i];
            if (!isServerDN(dn))
                return dn;
        }
        return null;
    }
    
    public static String[] getComponents(String dn, String componentName) {
        return ProxiedEntityUtils.getComponents(dn, componentName);
    }
    
    public static String normalizeDN(String userName) {
        return ProxiedEntityUtils.normalizeDN(userName);
    }
    
    /**
     * Encapsulates the known/valid NPE OU list, and whatever else is needed for NPE handling. Static inner-class, so that injection of the configured OUs is
     * only performed on-demand/if needed
     */
    public static class NpeUtils {
        
        /** Property containing a comma-delimited list of OUs */
        public static final String NPE_OU_PROPERTY = "npe.ou.entries";
        
        /** Parsed NPE OU identifiers */
        static final List<String> NPE_OU_LIST;
        
        private NpeUtils() {}
        
        static {
            List<String> npeOUs = new ArrayList<>();
            String ouString = System.getProperty(NPE_OU_PROPERTY, PROPS.getProperty(NPE_OU_PROPERTY));
            if (null == ouString || ouString.isEmpty()) {
                throw new IllegalStateException("No '" + NPE_OU_PROPERTY + "' value has been configured");
            }
            // Normalize and load...
            String[] ouArray = ouString.split(",");
            for (String ou : ouArray) {
                npeOUs.add(ou.trim().toUpperCase());
            }
            NPE_OU_LIST = Collections.unmodifiableList(npeOUs);
        }
        
        static boolean isNPE(String dn) {
            String[] ouList = ProxiedEntityUtils.getOrganizationalUnits(dn);
            for (String ou : ouList) {
                if (NPE_OU_LIST.contains(ou.toUpperCase())) {
                    return true;
                }
            }
            return false;
        }
    }
}
