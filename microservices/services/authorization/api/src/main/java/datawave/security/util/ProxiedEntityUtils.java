package datawave.security.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Pattern;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxiedEntityUtils {
    private static final Logger log = LoggerFactory.getLogger(ProxiedEntityUtils.class);
    
    public static String[] splitProxiedDNs(String proxiedDNs, boolean allowDups) {
        String[] dns;
        if (proxiedDNs.indexOf('<') < 0) {
            dns = new String[] {proxiedDNs};
        } else {
            Collection<String> dnCollection = allowDups ? new ArrayList<>() : new LinkedHashSet<>();
            String[] pieces = proxiedDNs.split("(?<!\\\\)<|(?<!\\\\)>");
            for (String piece : pieces) {
                if (piece.trim().length() > 0)
                    dnCollection.add(piece);
            }
            
            dns = dnCollection.toArray(new String[0]);
        }
        return dns;
    }
    
    public static String[] splitProxiedSubjectIssuerDNs(String proxiedDNs) {
        String[] dns;
        if (proxiedDNs.indexOf('<') < 0) {
            dns = new String[] {proxiedDNs};
        } else {
            HashSet<String> subjects = new HashSet<>();
            List<String> dnList = new ArrayList<>();
            String[] pieces = proxiedDNs.split("(?<!\\\\)<|(?<!\\\\)>");
            ArrayList<String> trimmedPieces = new ArrayList<>(pieces.length);
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
            
            dns = dnList.toArray(new String[0]);
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
    
    public static String getCommonName(String dn) {
        String[] comps = getComponents(dn, "CN");
        return comps.length >= 1 ? comps[0] : null;
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
    
    public static String[] getComponents(String dn, String componentName) {
        componentName = componentName.toUpperCase();
        ArrayList<String> components = new ArrayList<>();
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
        return components.toArray(new String[0]);
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
                ArrayList<Rdn> rdns = new ArrayList<>(name.getRdns());
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
}
