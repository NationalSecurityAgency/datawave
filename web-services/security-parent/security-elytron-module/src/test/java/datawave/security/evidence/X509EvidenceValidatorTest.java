package datawave.security.evidence;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

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
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.cert.X509CertificateVerifier;

class X509EvidenceValidatorTest {

    private static X509Certificate certificate;
    private static X509CertificateEvidence evidence;

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

        List<SubjectIssuerDNPair> entities = new ArrayList<>();
        entities.add(SubjectIssuerDNPair.of("cn=server", "cn=issuer"));
        entities.add(SubjectIssuerDNPair.of("cn=certuser", "cn=certissuer"));
        evidence = new X509CertificateEvidence("cn=server<cn=issuer><cn=certuser><cn=certissuer>", entities, certificate);
    }

    @Test
    void testValidateGivenSuccess() {
        X509CertificateVerifier verifier = Mockito.mock(X509CertificateVerifier.class);
        when(verifier.verify(certificate, "CN=certIssuer", null, null)).thenReturn(true);

        X509EvidenceValidator validator = new X509EvidenceValidator(verifier, null, null);
        assertTrue(validator.validate(evidence));
    }

    @Test
    void testValidateGivenFailure() {
        X509CertificateVerifier verifier = Mockito.mock(X509CertificateVerifier.class);
        when(verifier.verify(certificate, "CN=certIssuer", null, null)).thenReturn(false);

        X509EvidenceValidator validator = new X509EvidenceValidator(verifier, null, null);
        assertFalse(validator.validate(evidence));
    }
}
