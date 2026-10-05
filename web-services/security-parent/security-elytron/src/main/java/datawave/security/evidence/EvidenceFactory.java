package datawave.security.evidence;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;

import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnProperties;
import datawave.security.util.DnUtils;
import datawave.security.util.SecurityConstants;

/**
 * A factory for creating {@link org.wildfly.security.evidence.Evidence} instances in a standardized manner. This factory should be used when we need to create
 * a piece of evidence to programmatically obtain a {@link org.wildfly.security.auth.server.SecurityIdentity} for the security domain "datawave". See the
 * following example where a {@link TrustedHeaderEvidence} is created and used to obtain a security identity.
 *
 * <pre>
 * String subjectDn = "cn=Test A. User, c=US, o=Example Corp, ou=Example Developers";
 * String issuerDn = "cn=EXAMPLE CORP CA, c=US, o=Example Corp";
 * TrustedHeaderEvidence evidence = EvidenceFactory.getDefault().createTrustedHeaderEvidence(subjectDn, issuerDn, null, null);
 * SecurityDomain domain = SecurityDomain.getCurrent();
 * SecurityIdentity identity = domain.authenticate(evidence);
 * identity.runAs((Callable&lt;Void&gt;) -&gt; {
 *     // Operations to execute as the authenticated user.
 * })
 * </pre>
 */
public class EvidenceFactory {

    private static final EvidenceFactory defaultInstance = of(System.getProperty(SecurityConstants.TRUSTED_PROXIED_ENTITIES_SYSTEM_PROPERTY));

    /**
     * Return the default instance of {@link EvidenceFactory} where trusted proxied entities were extracted from the system property
     * {@value SecurityConstants#TRUSTED_PROXIED_ENTITIES_SYSTEM_PROPERTY}.
     *
     * @return the default {@link EvidenceFactory} instance
     */
    public static EvidenceFactory getDefault() {
        return defaultInstance;
    }

    /**
     * Create and return an {@link EvidenceFactory} with trusted proxied entities parsed from the given string.
     *
     * @param trustedProxiedEntities
     *            the trusted proxied entities
     * @return the new {@link EvidenceFactory} instance
     */
    public static EvidenceFactory of(String trustedProxiedEntities) {
        if (trustedProxiedEntities != null && !trustedProxiedEntities.isBlank()) {
            // @formatter:off
            Set<String> entities = Arrays.stream(DnUtils.splitProxiedDNs(trustedProxiedEntities, false))
                            .map(String::toLowerCase)
                            .collect(Collectors.toSet());
            // @formatter:on
            return new EvidenceFactory(entities);
        } else {
            return new EvidenceFactory();
        }
    }

    /**
     * The set of trusted proxied entities.
     */
    private final Set<String> trustedProxiedEntities;

    public EvidenceFactory() {
        this(null);
    }

    public EvidenceFactory(Set<String> trustedProxiedEntities) {
        this.trustedProxiedEntities = trustedProxiedEntities == null ? Set.of() : Set.copyOf(trustedProxiedEntities);
    }

    /**
     * Return the set of trusted proxied entities configured for this {@link EvidenceFactory}.
     *
     * @return the trusted proxied entities
     */
    public Set<String> getTrustedProxiedEntities() {
        return trustedProxiedEntities;
    }

    /**
     * Create and return a new {@link JWTEvidence} with the given token.
     *
     * @param token
     *            the JSON web token
     * @return the evidence
     */
    public JWTEvidence createJwtEvidence(String token) {
        Preconditions.checkArgument((token != null && !token.isBlank()), "token must not be null or blank");
        return new JWTEvidence(token.trim());
    }

    /**
     * Create and return a new {@link TrustedHeaderEvidence} created from the given subject DN, issuer DN, and proxied subjects and issuers. Any trusted proxied
     * entities configured for this {@link EvidenceFactory} will be pruned from the final set of entities in the returned evidence.
     *
     * @param subjectDn
     *            the subject DN
     * @param issuerDn
     *            the issuer DN
     * @param proxiedSubjects
     *            the proxied subject DNs
     * @param proxiedIssuers
     *            the proxied issuer DNs
     * @return the evidence
     */
    public TrustedHeaderEvidence createTrustedHeadersEvidence(String subjectDn, String issuerDn, String proxiedSubjects, String proxiedIssuers) {
        Preconditions.checkArgument((subjectDn != null && !subjectDn.isBlank()), "subject DN must not be null or blank");
        Preconditions.checkArgument((issuerDn != null && !issuerDn.isBlank()), "issuer DN must not be null or blank");

        Pair<String,List<SubjectIssuerDNPair>> pair = getUsernameAndEntities(subjectDn.trim(), issuerDn.trim(), proxiedSubjects, proxiedIssuers);
        return new TrustedHeaderEvidence(pair.getLeft(), pair.getRight());
    }

    /**
     * Create and return a new {@link X509CertificateEvidence} created from the given certificate and proxied subjects and issuers. Any trusted proxied entities
     * configured for this {@link EvidenceFactory} will be pruned from the final set of entities in the returned evidence.
     *
     * @param certificate
     *            the certificate
     * @param proxiedSubjects
     *            the proxied subject DNs
     * @param proxiedIssuers
     *            the proxied issuer DNs
     * @return the evidence
     */
    public X509CertificateEvidence createX509CertificateEvidence(X509Certificate certificate, String proxiedSubjects, String proxiedIssuers) {
        Preconditions.checkNotNull(certificate, "certificate must not be null");

        Pair<String,List<SubjectIssuerDNPair>> pair = getUsernameAndEntities(certificate.getSubjectX500Principal().getName(),
                        certificate.getIssuerX500Principal().getName(), proxiedSubjects, proxiedIssuers);
        return new X509CertificateEvidence(pair.getLeft(), pair.getRight(), certificate);
    }

    /**
     * Extract and return the username and entities from the given information.
     *
     * @param subjectDn
     *            the subject DN
     * @param issuerDn
     *            the issuer DN
     * @param proxiedSubjects
     *            the proxied subjects
     * @param proxiedIssuers
     *            the proxied issuers
     * @return a {@link Pair} with the username (left) and the entities (right)
     */
    private Pair<String,List<SubjectIssuerDNPair>> getUsernameAndEntities(String subjectDn, String issuerDn, String proxiedSubjects, String proxiedIssuers) {
        Preconditions.checkArgument(proxiedSubjects == null || proxiedIssuers != null, "proxied subjects provided without proxied issuers");
        Preconditions.checkArgument(proxiedIssuers == null || proxiedSubjects != null, "proxied issuers provided without proxied subjects");

        List<SubjectIssuerDNPair> entities = new ArrayList<>();

        // Extract the proxied entities (if any).
        if (proxiedSubjects != null && !proxiedSubjects.isBlank()) {
            String[] subjects = DnUtils.splitProxiedDNs(proxiedSubjects, true);
            String[] issuers = DnUtils.splitProxiedDNs(proxiedIssuers, true);
            if (subjects.length != issuers.length) {
                throw new IllegalArgumentException(
                                "Unequal number of proxied subjects and issuers. Subjects=" + proxiedSubjects + ", Issuers" + "=" + proxiedIssuers);
            }
            for (int i = 0; i < subjects.length; ++i) {
                entities.add(SubjectIssuerDNPair.of(subjects[i], issuers[i]));
            }
        }

        // Add an entity with the subject DN and issuer DN to the entity list.
        entities.add(SubjectIssuerDNPair.of(subjectDn, issuerDn));
        String username = DnUtils.buildNormalizedProxyDN(subjectDn, issuerDn, proxiedSubjects, proxiedIssuers,
                        DnProperties.getDefaultInstance().getSubjectDnPattern());

        // If the entities contain any trusted proxied entities, remove them from the final entity set.
        if (!trustedProxiedEntities.isEmpty()) {
            int originalSize = entities.size();
            entities = entities.stream().filter(entity -> !trustedProxiedEntities.contains(entity.subjectDN().toLowerCase())).collect(Collectors.toList());
            // If any entities were pruned, rebuild the username.
            if (originalSize != entities.size()) {
                username = DnUtils.buildNormalizedProxyDN(entities);
            }
        }

        return Pair.of(username, entities);
    }
}
