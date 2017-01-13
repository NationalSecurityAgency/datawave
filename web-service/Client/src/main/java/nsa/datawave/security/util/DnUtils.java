package nsa.datawave.security.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import org.apache.log4j.Logger;

public class DnUtils {
    
    /** Config for injecting NPE OU identifiers */
    public static final String PROPS_RESOURCE = "dnutils.properties";
    
    private static final Properties PROPS = new Properties();
    
    private static final Pattern SUBJECT_DN_PATTERN = Pattern.compile("(?:^|,)\\s*OU\\s*=\\s*D[0-9]{3}\\s*(?:,|$)", Pattern.CASE_INSENSITIVE);
    
    private static final Logger log = Logger.getLogger(DnUtils.class);
    
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
    }
    
    public static String[] splitProxiedDNs(String proxiedDNs, boolean allowDups) {
        String[] dns;
        if (proxiedDNs.indexOf('<') < 0) {
            dns = new String[] {proxiedDNs};
        } else {
            Collection<String> dnCollection = allowDups ? new ArrayList<String>() : new LinkedHashSet<String>();
            String[] pieces = proxiedDNs.split("(?<!\\\\)<|(?<!\\\\)>");
            for (String piece : pieces) {
                if (piece.trim().length() > 0)
                    dnCollection.add(piece);
            }
            
            dns = dnCollection.toArray(new String[dnCollection.size()]);
        }
        return dns;
    }
    
    public static String[] splitProxiedSubjectIssuerDNs(String proxiedDNs) {
        String[] dns;
        if (proxiedDNs.indexOf('<') < 0) {
            dns = new String[] {proxiedDNs};
        } else {
            HashSet<String> subjects = new HashSet<String>();
            List<String> dnList = new ArrayList<String>();
            String[] pieces = proxiedDNs.split("(?<!\\\\)<|(?<!\\\\)>");
            ArrayList<String> trimmedPieces = new ArrayList<String>(pieces.length);
            for (String piece : pieces) {
                piece = piece.trim();
                if (piece.length() > 0)
                    trimmedPieces.add(piece);
            }
            if ((trimmedPieces.size() % 2) != 0)
                throw new IllegalArgumentException("Invalid proxied DNs list does not have entries in pairs.");
            for (int i = 0; i < trimmedPieces.size(); i += 2) {
                String subject = trimmedPieces.get(i);
                String issuer = trimmedPieces.get(i + 1);
                if (subject.length() > 0 && !subjects.contains(subject)) {
                    subjects.add(subject);
                    dnList.add(subject);
                    dnList.add(issuer);
                }
            }
            
            dns = dnList.toArray(new String[dnList.size()]);
        }
        return dns;
    }
    
    public static String buildProxiedDN(String... dns) {
        StringBuilder sb = new StringBuilder();
        for (String dn : dns) {
            String escapedDN = dn.replaceAll("(?<!\\\\)(<|>)", "\\\\$1");
            if (sb.length() == 0)
                sb.append(escapedDN);
            else
                sb.append('<').append(escapedDN).append('>');
        }
        return sb.toString();
    }
    
    public static Collection<String> buildNormalizedDNList(String subjectDN, String issuerDN, String proxiedSubjectDNs, String proxiedIssuerDNs) {
        subjectDN = normalizeDN(subjectDN);
        issuerDN = normalizeDN(issuerDN);
        List<String> dnList = new ArrayList<String>();
        HashSet<String> subjects = new HashSet<String>();
        String subject = subjectDN.replaceAll("(?<!\\\\)(<|>)", "\\\\$1");
        dnList.add(subject);
        subjects.add(subject);
        dnList.add(issuerDN.replaceAll("(?<!\\\\)(<|>)", "\\\\$1"));
        if (proxiedSubjectDNs != null) {
            if (proxiedIssuerDNs == null)
                throw new IllegalArgumentException("If proxied subject DNs are supplied, then issuer DNs must be supplied as well.");
            String[] subjectDNarray = splitProxiedDNs(proxiedSubjectDNs, true);
            String[] issuerDNarray = splitProxiedDNs(proxiedIssuerDNs, true);
            if (subjectDNarray.length != issuerDNarray.length)
                throw new IllegalArgumentException("Subject and isser DN lists do not have the same number of entries: " + Arrays.toString(subjectDNarray)
                                + " vs " + Arrays.toString(issuerDNarray));
            for (int i = 0; i < subjectDNarray.length; ++i) {
                subjectDNarray[i] = normalizeDN(subjectDNarray[i]);
                if (!subjects.contains(subjectDNarray[i])) {
                    issuerDNarray[i] = normalizeDN(issuerDNarray[i]);
                    subjects.add(subjectDNarray[i]);
                    dnList.add(subjectDNarray[i]);
                    dnList.add(issuerDNarray[i]);
                    if (issuerDNarray[i].equalsIgnoreCase(subjectDNarray[i]))
                        throw new IllegalArgumentException("Subject DN " + issuerDNarray[i] + " was passed as an issuer DN.");
                    if (SUBJECT_DN_PATTERN.matcher(issuerDNarray[i]).find())
                        throw new IllegalArgumentException("It appears that a subject DN (" + issuerDNarray[i] + ") was passed as an issuer DN.");
                }
            }
        }
        return dnList;
    }
    
    public static String buildNormalizedProxyDN(String subjectDN, String issuerDN, String proxiedSubjectDNs, String proxiedIssuerDNs) {
        subjectDN = normalizeDN(subjectDN);
        issuerDN = normalizeDN(issuerDN);
        List<String> dnList = new ArrayList<String>();
        HashSet<String> subjects = new HashSet<String>();
        String subject = subjectDN.replaceAll("(?<!\\\\)(<|>)", "\\\\$1");
        dnList.add(subject);
        subjects.add(subject);
        dnList.add(issuerDN.replaceAll("(?<!\\\\)(<|>)", "\\\\$1"));
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
                if (!subjects.contains(subjectDNarray[i])) {
                    issuerDNarray[i] = normalizeDN(issuerDNarray[i]);
                    subjects.add(subjectDNarray[i]);
                    dnList.add(subjectDNarray[i]);
                    dnList.add(issuerDNarray[i]);
                    if (issuerDNarray[i].equalsIgnoreCase(subjectDNarray[i]))
                        throw new IllegalArgumentException("Subject DN " + issuerDNarray[i] + " was passed as an issuer DN.");
                    if (SUBJECT_DN_PATTERN.matcher(issuerDNarray[i]).find())
                        throw new IllegalArgumentException("It appears that a subject DN (" + issuerDNarray[i] + ") was passed as an issuer DN.");
                }
            }
        }
        
        StringBuilder sb = new StringBuilder();
        for (String escapedDN : dnList) {
            if (sb.length() == 0)
                sb.append(escapedDN);
            else
                sb.append('<').append(escapedDN).append('>');
        }
        return sb.toString();
    }
    
    public static String getCommonName(String dn) {
        String[] comps = getComponents(dn, "CN");
        return (comps != null && comps.length >= 1) ? comps[0] : null;
    }
    
    public static String[] getOrganizationalUnits(String dn) {
        return getComponents(dn, "OU");
    }
    
    public static String getShortName(String dn) {
        String cn = getCommonName(dn);
        if (cn == null)
            cn = dn;
        String sid = cn;
        int idx = cn.lastIndexOf(' ');
        if (idx >= 0)
            sid = cn.substring(idx + 1);
        return sid;
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
        componentName = componentName.toUpperCase();
        ArrayList<String> components = new ArrayList<String>();
        try {
            LdapName name = new LdapName(dn);
            for (Rdn rdn : name.getRdns()) {
                if (componentName.equals(rdn.getType().toUpperCase())) {
                    components.add(String.valueOf(rdn.getValue()));
                }
            }
        } catch (InvalidNameException e) {
            // ignore -- invalid name, so can't find components
        }
        return components.toArray(new String[components.size()]);
    }
    
    /**
     * Attempts to normalize a DN by taking it and reversing the components if it doesn't start with CN. Some systems requires the DN components be in a
     * specific order, or that order reversed. We cannot arbitrarily reorder the components however, e.g., sorting them.
     */
    public static String normalizeDN(String userName) {
        String normalizedUserName = userName.trim().toLowerCase();
        try {
            if (!normalizedUserName.startsWith("cn") || Pattern.compile(",[^ ]").matcher(normalizedUserName).find()) {
                LdapName name = new LdapName(userName);
                StringBuilder sb = new StringBuilder();
                ArrayList<Rdn> rdns = new ArrayList<Rdn>(name.getRdns());
                if (rdns.size() > 0 && !rdns.get(0).toString().toLowerCase().startsWith("cn"))
                    Collections.reverse(rdns);
                for (Rdn rdn : rdns) {
                    if (sb.length() > 0)
                        sb.append(", ");
                    sb.append(rdn.toString());
                }
                normalizedUserName = sb.toString().toLowerCase();
            }
        } catch (InvalidNameException e) {
            // ignore -- might be a sid rather than a DN
        }
        log.trace("Normalized [" + userName + "] into [" + normalizedUserName + "]");
        return normalizedUserName;
    }
    
    /**
     * Encapsulates the known/valid NPE OU list, and whatever else is needed for NPE handling Static inner-class, so that injection of the configured OUs is
     * only performed on-demand/if needed
     */
    private static class NpeUtils {
        
        /** Property containing a comma-delimited list of OUs */
        static final String NPE_OU_PROPERTY = "npe.ou.entries";
        
        /** Parsed NPE OU identifiers */
        static final List<String> NPE_OU_LIST;
        
        private NpeUtils() {}
        
        static {
            List<String> npeOUs = new ArrayList<String>();
            String ouString = PROPS.getProperty(NPE_OU_PROPERTY);
            if (null == ouString || ouString.isEmpty()) {
                throw new IllegalStateException(PROPS_RESOURCE + " contains no '" + NPE_OU_PROPERTY + "' property");
            }
            // Normalize and load...
            String[] ouArray = ouString.split(",");
            for (String ou : ouArray) {
                npeOUs.add(ou.trim().toUpperCase());
            }
            NPE_OU_LIST = Collections.unmodifiableList(npeOUs);
        }
        
        static boolean isNPE(String dn) {
            String[] ouList = getOrganizationalUnits(dn);
            if (ouList != null) {
                for (String ou : ouList) {
                    if (NPE_OU_LIST.contains(ou.toUpperCase())) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
