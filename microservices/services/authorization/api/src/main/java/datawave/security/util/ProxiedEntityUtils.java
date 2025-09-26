package datawave.security.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides utility functions for splitting and extracting components from DNs.
 */
public class ProxiedEntityUtils {

    private static final Logger log = LoggerFactory.getLogger(ProxiedEntityUtils.class);

    /**
     * Split the given string into proxied DNs and return them. It is expected that the DN string matches the format returned by
     * {@link ProxiedEntityUtils#buildProxiedDN(String...)}, i,e, that the DNs should be in the format {@code "dn1<dn2><dn3>...<dnN>"}.
     *
     * @param proxiedDNs
     *            the proxied DNs
     * @param allowDuplicates
     *            Whether duplicate DNs should be retained
     * @return an array containing the individual DNs, in original order
     */
    public static String[] splitProxiedDNs(String proxiedDNs, boolean allowDuplicates) {
        String[] dns;
        // If the string does not contain any occurrences of <, we can assume that there is only one DN in the string, so long as it was created via
        // ProxiedEntityUtils.buildProxiedDns().
        if (proxiedDNs.indexOf('<') < 0) {
            dns = new String[] {proxiedDNs};
        } else {
            // @formatter:off
            // Split on all occurrences of < and > that are not escaped.
            Stream<String> stream = Arrays.stream(proxiedDNs.split("(?<!\\\\)<|(?<!\\\\)>"))
                            .filter(s -> !s.isBlank()); // Filter out all blank strings.
            // @formatter:on
            if (!allowDuplicates) {
                // If duplicates are not allowed, remove them.
                stream = stream.distinct();
            }
            dns = stream.toArray(String[]::new);
        }
        return dns;
    }

    /**
     * Split the given string into proxied subject and issuer pairs and return them. It is expected that the DN string matches the format returned by
     * {@link ProxiedEntityUtils#buildProxiedDN(String...)}, i,e, that the DNs should be in the format {@code "dn1<dn2><dn3>...<dnN>"}. If an uneven number of
     * DNs greater than 1 is found, an {@link IllegalArgumentException} will be thrown. Only the first pairing for any distinct subject DNs will be retained.
     *
     * @param proxiedDNs
     *            the proxied DNs
     * @return an array containing the subject and issuer DNs, in original order
     */
    public static String[] splitProxiedSubjectIssuerDNs(String proxiedDNs) {
        String[] dns = splitProxiedDNs(proxiedDNs, true);
        // If only up to one DN was found, return the DN.
        if (dns.length < 2) {
            return dns;
        }

        // Verify that there are an even number of DNs.
        if (dns.length % 2 != 0) {
            throw new IllegalArgumentException("Invalid proxied DNs list does not have entries in pairs.");
        }

        // Retain only the first pairing for each distinct subject DN seen.
        Set<String> subjectsSeen = new HashSet<>();
        ArrayList<String> distinctPairs = new ArrayList<>(dns.length);
        for (int i = 0; i < dns.length; i += 2) {
            String subject = dns[i];
            if (!subjectsSeen.contains(subject)) {
                distinctPairs.add(subject); // subject DN
                distinctPairs.add(dns[i + 1]); // issuer DN
                subjectsSeen.add(subject);
            }
        }

        return distinctPairs.toArray(new String[0]);
    }

    /**
     * Returns a string with the given DNs concatenated. Any occurrence of the characters {@code <} and {@code >} will be escaped with a backslash. All DNs
     * after the first DN will be prefixed with {@code <} and suffixed with {@code >} before concatenation, e.g., the arguments {@code {"dn1", "dn2", "dn3:"}}
     * would result in {@code "dn1<dn2><dn3>"}.
     *
     * @param dns
     *            the dns to build the proxied DN with
     * @return the concatenated DNs, or the original string if a single blank string is provided
     */
    public static String buildProxiedDN(String... dns) {
        StringBuilder sb = new StringBuilder();
        for (String dn : dns) {
            // Escape all occurrences of < and > with a backslash.
            String escapedDN = dn.replaceAll("(?<!\\\\)([<>])", "\\\\$1");
            if (sb.length() == 0)
                sb.append(escapedDN);
            else
                // Wrap all DNs other than the first one with < and >.
                sb.append('<').append(escapedDN).append('>');
        }
        return sb.toString();
    }

    /**
     * Returns the value of the last common name (CN) in the given DN. Returns null iff the DN is blank, contains no CN, or cannot be parsed as a DN.
     *
     * @param dn
     *            the DN
     * @return the value of the last CN in the given DN.
     */
    public static String getCommonName(String dn) {
        String[] comps = getComponents(dn, "CN");
        return comps.length >= 1 ? comps[0] : null;
    }

    /**
     * Returns the values of the organizational units (OU) in the given DN in reverse order. Returns an empty array iff the DN is blank, contains no OU, or
     * cannot be parsed as a DN.
     *
     * @param dn
     *            the DN
     * @return the organizational units in the given DN
     */
    public static String[] getOrganizationalUnits(String dn) {
        return getComponents(dn, "OU");
    }

    /**
     * Attempts to return the short name derived from CNs found in the given DN. The value returned will fall into one of the following scenarios:
     * <ul>
     * <li>If the DN contains a single CN, the substring after the last space in the CN will be returned, e.g., the DN
     * {@code "cn=john q. doe, c=us, o=my org, ou=my dept"} will return {@code "doe"}.</li>
     * <li>If the DN contains a single CN, the substring after the last space in the last CN will be returned, e.g., the DN
     * {@code "cn=johnny q. doe, cn=jonathan q. buck, cn=john q. hart, c=us, o=my org, ou=my dept"} will return {@code "hart"}.</li>
     * <li>If the DN contains no CN, the substring after the last space will be returned.</li>
     * <li>If the DN cannot be parsed as a CN, e.g., it is a SID, the substring after the last space will be returned.</li>
     * <li>If the string is blank, an empty string will be returned.</li>
     * </ul>
     *
     * @param dn
     *            the DN
     * @return the best guess at the short name for the DN.
     */
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

    /**
     * Returns an array containing the value of any RDNs in the supplied DNs that match the given RDN name. RDN name matching is case-insensitive. If the DN is
     * blank, the RDN name is blank, or the DN cannot be parsed as an LDAP name, an empty array will be returned. If multiple instances of the RDN are found in
     * the component, their values be returned in reverse order as they appear in the DN.
     *
     * @param dn
     *            the DN to examine
     * @param rdnName
     *            the RDN (component) name
     * @return a string array containing the values of matching components
     */
    public static String[] getComponents(String dn, String rdnName) {
        // Return an empty array if either the DN or component name is blank.
        if (dn.isBlank() || rdnName.isBlank()) {
            return new String[0];
        }

        // Extract the values for any matching RDN.
        rdnName = rdnName.toUpperCase();
        ArrayList<String> components = new ArrayList<>();
        try {
            LdapName name = new LdapName(dn);
            for (Rdn rdn : name.getRdns()) {
                if (rdnName.equals(rdn.getType().toUpperCase())) {
                    components.add(String.valueOf(rdn.getValue()));
                }
            }
        } catch (InvalidNameException e) {
            // ignore -- invalid name, so can't find components
        }

        return components.toArray(new String[0]);
    }

    /**
     * Regex patterns to match against a comma followed by a space.
     */
    private static final Pattern COMMA_SPACE_PATTERN = Pattern.compile(",[^ ]");

    /**
     * Attempts to normalize a DN by taking it and reversing the components if it doesn't start with CN. Some systems require the DN components be in a specific
     * order, or that order reversed. We cannot arbitrarily reorder the components, however, e.g., such as by sorting them. The username returned will fall into
     * one of the following scenarios:
     * <ul>
     * <li>If the username is a DN and the last RDN is the CN, a DN with the RDNs reversed, trimmed of whitespace and all lowercase will be returned.</li>
     * <li>If the username is a DN and the last RDN is not a CN, the original DN, trimmed of whitespace and all lowercase will be returned.</li>
     * <li>If the username cannot be parsed as a DN, the original username, trimmed of whitespace and all lowercase will be returned.</li>
     * </ul>
     */
    public static String normalizeDN(String userName) {
        String normalizedUserName = userName.trim().toLowerCase();
        try {
            // If the username does not start with cn or contains a comma directly followed by a string, parse the dn as a LDAPName and determine if we should
            // reverse the RDNs in the DN.
            if (!normalizedUserName.startsWith("cn") || COMMA_SPACE_PATTERN.matcher(normalizedUserName).find()) {
                LdapName name = new LdapName(userName);
                ArrayList<Rdn> rdns = new ArrayList<>(name.getRdns());
                // If the last RDN in the original DN, now the first RDN in the list, is the CN, reverse the RDN list.
                if (!rdns.isEmpty() && !rdns.get(0).toString().toLowerCase().startsWith("cn")) {
                    Collections.reverse(rdns);
                }

                // Reconstruct the username string.
                normalizedUserName = rdns.stream().map(Object::toString).collect(Collectors.joining(", ")).toLowerCase();
            }
        } catch (InvalidNameException e) {
            // ignore -- might be a sid rather than a DN
        }
        if (log.isTraceEnabled()) {
            log.trace("Normalized [{}] into [{}]", userName, normalizedUserName);
        }
        return normalizedUserName;
    }

    /**
     * Do not allow this class to be instantiated.
     */
    private ProxiedEntityUtils() {
        throw new UnsupportedOperationException();
    }
}
