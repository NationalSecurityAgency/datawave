package datawave.security.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;

import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.SecurityConstants;

@SetSystemProperty(key = SecurityConstants.TRUSTED_PROXIED_ENTITIES_SYSTEM_PROPERTY,
                value = "cn=server1, c=\\<us\\>, o=my org, ou=\\<my dept\\><cn=server2, c=us, o=my org, ou=my dept>")
class EvidenceFactoryTest {

    private static X509Certificate certificate;

    @BeforeAll
    static void beforeAll() throws Exception {
        Security.addProvider(new BouncyCastleProvider());

        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA", "BC");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();

        X500Name subjectDn = new X500Name("CN=certUser");
        X500Name issuerDn = new X500Name("CN=certIssuer");
        BigInteger serialNumber = BigInteger.valueOf(new SecureRandom().nextInt());
        Date validFrom = new Date();
        Date validTo = new Date(validFrom.getTime() + 365 * 25 * 60 * 60 * 1000L);

        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(issuerDn, serialNumber, validFrom, validTo, subjectDn, keyPair.getPublic());
        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").setProvider("BC").build(keyPair.getPrivate());
        certificate = new JcaX509CertificateConverter().setProvider("BC").getCertificate(builder.build(signer));
    }

    /**
     * Verify that {@link EvidenceFactory#getDefault()} returns an instance with trusted proxied entities parsed from the system property
     * {@value SecurityConstants#TRUSTED_PROXIED_ENTITIES_SYSTEM_PROPERTY}.
     */
    @Test
    void testDefaultInstance() {
        EvidenceFactory factory = EvidenceFactory.getDefault();
        assertThat(factory.getTrustedProxiedEntities()).containsExactlyInAnyOrder("cn=server1, c=\\<us\\>, o=my org, ou=\\<my dept\\>",
                        "cn=server2, c=us, o=my org, ou=my dept");
    }

    /**
     * Verify that {@link EvidenceFactory#of(String)} has no trusted entities given a null string.
     */
    @Test
    void testOfGivenNull() {
        EvidenceFactory factory = EvidenceFactory.of(null);
        assertTrue(factory.getTrustedProxiedEntities().isEmpty());
    }

    /**
     * Verify that {@link EvidenceFactory#of(String)} has no trusted entities given a blank string.
     */
    @Test
    void testOfGivenBlank() {
        EvidenceFactory factory = EvidenceFactory.of("   ");
        assertTrue(factory.getTrustedProxiedEntities().isEmpty());
    }

    /**
     * Verify that {@link EvidenceFactory#of(String)} has trusted entities given string with entities.
     */
    @Test
    void testOfGivenEntities() {
        EvidenceFactory factory = EvidenceFactory.of("cn=server1, c=\\<us\\>, o=my org, ou=\\<my dept\\><cn=server2, c=us, o=my org, ou=my dept>");
        assertThat(factory.getTrustedProxiedEntities()).containsExactlyInAnyOrder("cn=server1, c=\\<us\\>, o=my org, ou=\\<my dept\\>",
                        "cn=server2, c=us, o=my org, ou=my dept");
    }

    /**
     * Tests for {@link EvidenceFactory#createJwtEvidence(String)}.
     */
    @Nested
    class CreateJwtEvidence {

        /**
         * Verify that {@link EvidenceFactory#createJwtEvidence(String)} throws an exception given a null token.
         */
        @Test
        void givenNullToken() {
            assertThatThrownBy(() -> EvidenceFactory.getDefault().createJwtEvidence(null)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("token must not be null or blank");
        }

        /**
         * Verify that {@link EvidenceFactory#createJwtEvidence(String)} throws an exception given a blank token.
         */
        @Test
        void givenBlankToken() {
            assertThatThrownBy(() -> EvidenceFactory.getDefault().createJwtEvidence("   ")).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("token must not be null or blank");
        }

        /**
         * Verify that {@link EvidenceFactory#createJwtEvidence(String)} throws an exception given a blank token.
         */
        @Test
        void givenValidToken() {
            JWTEvidence evidence = EvidenceFactory.getDefault().createJwtEvidence("token");
            assertEquals("token", evidence.getToken());
            assertEquals("token", evidence.getUsername());
        }
    }

    /**
     * Tests for {@link EvidenceFactory#createTrustedHeadersEvidence(String, String, String, String)}.
     */
    @Nested
    class CreateTrustedHeaderEvidence {

        /**
         * Verify that {@link EvidenceFactory#createTrustedHeadersEvidence(String, String, String, String)} throws an exception given a null subject DN.
         */
        @Test
        void givenNullSubjectDn() {
            assertThatThrownBy(() -> EvidenceFactory.getDefault().createTrustedHeadersEvidence(null, null, null, null))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("subject DN must not be null or blank");
        }

        /**
         * Verify that {@link EvidenceFactory#createTrustedHeadersEvidence(String, String, String, String)} throws an exception given a blank subject DN.
         */
        @Test
        void givenBlankSubjectDn() {
            assertThatThrownBy(() -> EvidenceFactory.getDefault().createTrustedHeadersEvidence("   ", null, null, null))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("subject DN must not be null or blank");
        }

        /**
         * Verify that {@link EvidenceFactory#createTrustedHeadersEvidence(String, String, String, String)} throws an exception given a null issuer DN.
         */
        @Test
        void givenNullIssuerDn() {
            assertThatThrownBy(() -> EvidenceFactory.getDefault().createTrustedHeadersEvidence("cn=user1", null, null, null))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("issuer DN must not be null or blank");
        }

        /**
         * Verify that {@link EvidenceFactory#createTrustedHeadersEvidence(String, String, String, String)} throws an exception given a blank issuer DN.
         */
        @Test
        void givenBlankIssuerDn() {
            assertThatThrownBy(() -> EvidenceFactory.getDefault().createTrustedHeadersEvidence("cn=user1", "   ", null, null))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("issuer DN must not be null or blank");
        }

        /**
         * Verify that {@link EvidenceFactory#createTrustedHeadersEvidence(String, String, String, String)} throws an exception given proxied subjects without
         * proxied issuers.
         */
        @Test
        void givenProxiedSubjectsWithoutProxiedIssuers() {
            assertThatThrownBy(() -> EvidenceFactory.getDefault().createTrustedHeadersEvidence("cn=user1", "cn=issuer1", "cn=proxiedUser1", null))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("proxied subjects provided without proxied issuers");
        }

        /**
         * Verify that {@link EvidenceFactory#createTrustedHeadersEvidence(String, String, String, String)}} throws an exception given proxied issuers without
         * proxied subjects.
         */
        @Test
        void givenProxiedIssuersWithoutProxiedSubjects() {
            assertThatThrownBy(() -> EvidenceFactory.getDefault().createTrustedHeadersEvidence("cn=user1", "cn=issuer1", null, "cn=proxiedIssuer1"))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("proxied issuers provided without proxied subjects");
        }

        /**
         * Verify that {@link EvidenceFactory#createTrustedHeadersEvidence(String, String, String, String)} throws an exception given an unequal number of
         * proxied subjects and issuers.
         */
        @Test
        void givenUnequalNumberOfProxiedSubjectsAndIssuers() {
            assertThatThrownBy(() -> EvidenceFactory.getDefault().createTrustedHeadersEvidence("cn=user1", "cn=issuer1",
                            "cn=proxiedSubject1<cn=proxiedSubject2>", "cn=proxiedIssuer1")).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Unequal number of proxied subjects and issuers. Subjects=cn=proxiedSubject1<cn=proxiedSubject2>, Issuers=cn=proxiedIssuer1");
        }

        /**
         * Verify that {@link EvidenceFactory#createTrustedHeadersEvidence(String, String, String, String)} prunes any trusted entities.
         */
        @Test
        void givenValidEntitiesWithTrustedEntitiesInChain() {
            TrustedHeaderEvidence evidence = EvidenceFactory.getDefault().createTrustedHeadersEvidence("cn=user1", "cn=issuer1",
                            "cn=server2, c=us, o=my org, ou=my dept<cn=server3, c=us, o=my org, ou=my dept>",
                            "cn=issuer2, c=us, o=my org, ou=my dept<cn=issuer3, c=us, o=my org, ou=my dept>");
            assertEquals("cn=server3, c=us, o=my org, ou=my dept<cn=issuer3, c=us, o=my org, ou=my dept><cn=user1><cn=issuer1>", evidence.getUsername());

            List<SubjectIssuerDNPair> entities = new ArrayList<>();
            entities.add(SubjectIssuerDNPair.of("cn=server3, c=us, o=my org, ou=my dept", "cn=issuer3, c=us, o=my org, ou=my dept"));
            entities.add(SubjectIssuerDNPair.of("cn=user1", "cn=issuer1"));
            assertEquals(entities, evidence.getEntities());
        }

        /**
         * Verify that {@link EvidenceFactory#createX509CertificateEvidence(X509Certificate, String, String)} does not prune any entities when there are no
         * trusted entities.
         */
        @Test
        void givenValidEntitiesWithNoTrustedEntitiesInChain() {
            TrustedHeaderEvidence evidence = EvidenceFactory.getDefault().createTrustedHeadersEvidence("cn=user1", "cn=issuer1", "cn=server4<cn=server5>",
                            "cn=issuer2<cn=issuer3>");
            assertEquals("cn=server4<cn=issuer2><cn=server5><cn=issuer3><cn=user1><cn=issuer1>", evidence.getUsername());

            List<SubjectIssuerDNPair> entities = new ArrayList<>();
            entities.add(SubjectIssuerDNPair.of("cn=server4", "cn=issuer2"));
            entities.add(SubjectIssuerDNPair.of("cn=server5", "cn=issuer3"));
            entities.add(SubjectIssuerDNPair.of("cn=user1", "cn=issuer1"));
            assertEquals(entities, evidence.getEntities());
        }
    }

    /**
     * Tests for {@link EvidenceFactory#createX509CertificateEvidence(X509Certificate, String, String)}.
     */
    @Nested
    class CreatedX509CertificateEvidence {

        /**
         * Verify that {@link EvidenceFactory#createX509CertificateEvidence(X509Certificate, String, String)} throws an exception given a null cert.
         */
        @Test
        void givenNullCertificate() {
            assertThatThrownBy(() -> EvidenceFactory.getDefault().createX509CertificateEvidence(null, null, null)).isInstanceOf(NullPointerException.class)
                            .hasMessage("certificate must not be null");
        }

        /**
         * Verify that {@link EvidenceFactory#createX509CertificateEvidence(X509Certificate, String, String)} throws an exception given proxied subjects without
         * proxied issuers.
         */
        @Test
        void givenProxiedSubjectsWithoutProxiedIssuers() {
            assertThatThrownBy(() -> EvidenceFactory.getDefault().createX509CertificateEvidence(certificate, "cn=proxiedUser1", null))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("proxied subjects provided without proxied issuers");
        }

        /**
         * Verify that {@link EvidenceFactory#createX509CertificateEvidence(X509Certificate, String, String)} throws an exception given proxied issuers without
         * proxied subjects.
         */
        @Test
        void givenProxiedIssuersWithoutProxiedSubjects() {
            assertThatThrownBy(() -> EvidenceFactory.getDefault().createX509CertificateEvidence(certificate, null, "cn=proxiedIssuer1"))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("proxied issuers provided without proxied subjects");
        }

        /**
         * Verify that {@link EvidenceFactory#createX509CertificateEvidence(X509Certificate, String, String)} throws an exception given an unequal number of
         * proxied subjects and issuers.
         */
        @Test
        void givenUnequalNumberOfProxiedSubjectsAndIssuers() {
            assertThatThrownBy(() -> EvidenceFactory.getDefault().createX509CertificateEvidence(certificate, "cn=proxiedSubject1<cn=proxiedSubject2>",
                            "cn=proxiedIssuer1")).isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Unequal number of proxied subjects and issuers. Subjects=cn=proxiedSubject1<cn=proxiedSubject2>, Issuers=cn=proxiedIssuer1");
        }

        /**
         * Verify that {@link EvidenceFactory#createX509CertificateEvidence(X509Certificate, String, String)} prunes any trusted entities.
         */
        @Test
        void givenValidEntitiesWithTrustedEntitiesInChain() {
            X509CertificateEvidence evidence = EvidenceFactory.getDefault().createX509CertificateEvidence(certificate,
                            "cn=server2, c=us, o=my org, ou=my dept<cn=server3, c=us, o=my org, ou=my dept>",
                            "cn=issuer2, c=us, o=my org, ou=my dept<cn=issuer3, c=us, o=my org, ou=my dept>");
            assertEquals("cn=server3, c=us, o=my org, ou=my dept<cn=issuer3, c=us, o=my org, ou=my dept><cn=certuser><cn=certissuer>", evidence.getUsername());

            List<SubjectIssuerDNPair> entities = new ArrayList<>();
            entities.add(SubjectIssuerDNPair.of("cn=server3, c=us, o=my org, ou=my dept", "cn=issuer3, c=us, o=my org, ou=my dept"));
            entities.add(SubjectIssuerDNPair.of("cn=certuser", "cn=certissuer"));
            assertEquals(entities, evidence.getEntities());
        }

        /**
         * Verify that {@link EvidenceFactory#createX509CertificateEvidence(X509Certificate, String, String)} does not prune any entities when there are no
         * trusted entities.
         */
        @Test
        void givenValidEntitiesWithNoTrustedEntitiesInChain() {
            X509CertificateEvidence evidence = EvidenceFactory.getDefault().createX509CertificateEvidence(certificate, "cn=server4<cn=server5>",
                            "cn=issuer2<cn=issuer3>");
            assertEquals("cn=server4<cn=issuer2><cn=server5><cn=issuer3><cn=certuser><cn=certissuer>", evidence.getUsername());

            List<SubjectIssuerDNPair> entities = new ArrayList<>();
            entities.add(SubjectIssuerDNPair.of("cn=server4", "cn=issuer2"));
            entities.add(SubjectIssuerDNPair.of("cn=server5", "cn=issuer3"));
            entities.add(SubjectIssuerDNPair.of("cn=certuser", "cn=certissuer"));
            assertEquals(entities, evidence.getEntities());
        }
    }
}
