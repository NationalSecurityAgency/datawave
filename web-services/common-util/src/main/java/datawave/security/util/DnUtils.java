package datawave.security.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.security.authorization.SubjectIssuerDNPair;

public class DnUtils {

    /** Config for injecting NPE OU identifiers */
    public static final String PROPS_RESOURCE = "dnutils.properties";

    private static final Properties PROPS = new Properties();

    public static final String SUBJECT_DN_PATTERN_PROPERTY = "subject.dn.pattern";

    private static final Pattern SUBJECT_DN_PATTERN;

    /** Property containing a comma-delimited list of OUs */
    public static final String NPE_OU_PROPERTY = "npe.ou.entries";

    /** Parsed NPE OU identifiers */
    static final List<String> NPE_OU_LIST;

    private static final Logger log = LoggerFactory.getLogger(DnUtils.class);

    private static final datawave.microservice.security.util.DnUtils dnUtils;

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

        dnUtils = new datawave.microservice.security.util.DnUtils(SUBJECT_DN_PATTERN, NPE_OU_LIST);
    }

    public static String[] splitProxiedDNs(String proxiedDNs, boolean allowDups) {
        return datawave.microservice.security.util.DnUtils.splitProxiedDNs(proxiedDNs, allowDups);
    }

    public static String[] splitProxiedSubjectIssuerDNs(String proxiedDNs) {
        return datawave.microservice.security.util.DnUtils.splitProxiedSubjectIssuerDNs(proxiedDNs);
    }

    public static String buildProxiedDN(String... dns) {
        return datawave.microservice.security.util.DnUtils.buildProxiedDN(dns);
    }

    public static Collection<String> buildNormalizedDNList(String subjectDN, String issuerDN, String proxiedSubjectDNs, String proxiedIssuerDNs) {
        return dnUtils.buildNormalizedDNList(subjectDN, issuerDN, proxiedSubjectDNs, proxiedIssuerDNs);
    }

    public static String buildNormalizedProxyDN(String subjectDN, String issuerDN, String proxiedSubjectDNs, String proxiedIssuerDNs) {
        return dnUtils.buildNormalizedProxyDN(subjectDN, issuerDN, proxiedSubjectDNs, proxiedIssuerDNs);
    }

    public static String buildNormalizedProxyDN(List<SubjectIssuerDNPair> dns) {
        return datawave.microservice.security.util.DnUtils.buildNormalizedProxyDN(dns);
    }

    public static String getCommonName(String dn) {
        return datawave.microservice.security.util.DnUtils.getCommonName(dn);
    }

    public static String[] getOrganizationalUnits(String dn) {
        return datawave.microservice.security.util.DnUtils.getOrganizationalUnits(dn);
    }

    public static String getShortName(String dn) {
        return datawave.microservice.security.util.DnUtils.getShortName(dn);
    }

    public static boolean isServerDN(String dn) {
        return dnUtils.isServerDN(dn);
    }

    public static String getUserDN(String[] dns) {
        return dnUtils.getUserDN(dns);
    }

    public static String getUserDN(String[] dns, boolean issuerDNs) {
        return dnUtils.getUserDN(dns, issuerDNs);
    }

    public static String[] getComponents(String dn, String componentName) {
        return dnUtils.getComponents(dn, componentName);
    }

    public static String normalizeDN(String userName) {
        return datawave.microservice.security.util.DnUtils.normalizeDN(userName);
    }
}
