package datawave.security.authorization;

import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import datawave.security.util.ProxiedEntityUtils;

/**
 * A simple pair containing a subject and (optional) issuer DN. The supplied DN values are normalized into a lower-case form with the CN portion first.
 *
 * @see ProxiedEntityUtils#normalizeDN(String)
 */
public class SubjectIssuerDNPair implements Serializable {

    private static final long serialVersionUID = -7558558154126871405L;

    private final String subjectDN;
    private final String issuerDN;

    /**
     * Return a new instance of {@link SubjectIssuerDNPair} with the given subject DN and a null issuer DN.
     *
     * @param subjectDN
     *            the subject DN
     * @return the new {@link SubjectIssuerDNPair}
     * @throws NullPointerException
     *             if subjectDN is null
     */
    public static SubjectIssuerDNPair of(String subjectDN) {
        return new SubjectIssuerDNPair(subjectDN, null);
    }

    /**
     * Return a new instance of {@link SubjectIssuerDNPair} with the given subject DN and issuer DN.
     *
     * @param subjectDN
     *            the subject DN
     * @param issuerDN
     *            the issuer DN
     * @return the new {@link SubjectIssuerDNPair}
     * @throws NullPointerException
     *             if subjectDN is null
     */
    @JsonCreator
    public static SubjectIssuerDNPair of(@JsonProperty("subjectDN") String subjectDN, @JsonProperty("issuerDN") String issuerDN) {
        return new SubjectIssuerDNPair(subjectDN, issuerDN);
    }

    /**
     * Parses and returns a new {@link SubjectIssuerDNPair} from the given string. It is expected that the string has the format {@code "subjectDN<issuerDN>"}.
     *
     * @param value
     *            the value to parse
     * @return the parsed {@link SubjectIssuerDNPair}
     * @throws IllegalArgumentException
     *             if DNs cannot be parsed from the argument, or if exactly two DNs are not parsed
     */
    public static SubjectIssuerDNPair parse(String value) {
        String[] dns = ProxiedEntityUtils.splitProxiedSubjectIssuerDNs(value);
        if (dns.length != 2) {
            throw new IllegalArgumentException("'" + value + "' must contain a single subject and issuer DN");
        }
        return new SubjectIssuerDNPair(dns[0], dns[1]);
    }

    protected SubjectIssuerDNPair(String subjectDN, String issuerDN) {
        Preconditions.checkNotNull(subjectDN, "Parameter subjectDN must not be null");
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
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubjectIssuerDNPair that = (SubjectIssuerDNPair) o;
        return Objects.equals(subjectDN, that.subjectDN) && Objects.equals(issuerDN, that.issuerDN);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subjectDN, issuerDN);
    }
}
