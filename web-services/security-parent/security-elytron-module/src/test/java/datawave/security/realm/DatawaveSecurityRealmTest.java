package datawave.security.realm;

import static datawave.security.realm.AttributeConstants.ATTRIBUTE_USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.security.auth.x500.X500Principal;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.wildfly.security.auth.principal.NamePrincipal;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.authz.Attributes;
import org.wildfly.security.evidence.Evidence;
import org.wildfly.security.evidence.PasswordGuessEvidence;

import com.google.common.collect.ImmutableMultimap;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.cache.ElytronCacheManager;
import datawave.security.cert.DatawaveCertVerifier;
import datawave.security.cert.SSLStores;
import datawave.security.cert.X509CertificateVerifier;
import datawave.security.evidence.TrustedHeaderEvidence;
import datawave.security.evidence.X509CertificateEvidence;
import datawave.security.system.SecurityEJBProvider;

/**
 * Tests for {@link DatawaveSecurityRealm}.
 */
class DatawaveSecurityRealmTest {

    private static final Set<String> auths = Set.of("A", "B", "C");
    private static final String ISSUER_DN = "cn=testissuer";
    private static final String USER_1_PRINCIPAL_NAME = "cn=testuserprincipal<" + ISSUER_DN + ">";
    private static final String USER_1_EVIDENCE_NAME = "cn=testuserevidence<" + ISSUER_DN + ">";

    @TempDir
    static Path tempDir;

    static Path propertiesFile;

    private Properties roleProperties;
    private final Map<String,String> configMap = new HashMap<>();
    private ElytronCacheManager cacheManager;
    private SecurityEJBProvider securityEJBProvider;

    @BeforeAll
    static void beforeAll() throws IOException {
        propertiesFile = Files.createFile(tempDir.resolve("roles.properties"));
    }

    @BeforeEach
    void setUp() {
        configMap.clear();
        roleProperties = new Properties();
        securityEJBProvider = Mockito.mock(SecurityEJBProvider.class);
        cacheManager = Mockito.mock(ElytronCacheManager.class);
        SSLStores sslStores = Mockito.mock(SSLStores.class);
        when(securityEJBProvider.getElytronCacheManager()).thenReturn(cacheManager);
        when(securityEJBProvider.getSSLStores()).thenReturn(sslStores);
    }

    /**
     * Verify that when the security realm is configured without an evidence validator or a role properties file, that it does not load those after completing
     * initialization.
     */
    @Test
    void testInitializationGivenNoValidatorOrRolePropertiesFile() throws Exception {
        DatawaveSecurityRealm realm = createSecurityRealm();

        // Call getRealmIdentity to trigger initialization.
        realm.getRealmIdentity(new NamePrincipal("username"));

        // Verify the identity cache was added to the cache manager.
        verify(cacheManager).addCache(any());

        // Verify a certificate validator was not created.
        assertNull(realm.getX509EvidenceValidator());

        // Verify that no local roles were loaded.
        assertTrue(realm.getLocalUserRoles().isEmpty());

        // Verify the identity cache is only ever added to the cache manager once, even on subsequent calls.
        realm.getRealmIdentity(new NamePrincipal("anotheruser"));
        verify(cacheManager, atMostOnce()).addCache(any());
    }

    /**
     * Verify that when the security realm is configured with an evidence validator and a role properties file, that it loads those after completing
     * initialization.
     */
    @Test
    void testInitializationGivenValidatorAndRolePropertiesFile() throws Exception {
        configMap.put(DatawaveSecurityRealm.Config.OPTION_CERT_VERIFIER, DatawaveCertVerifier.class.getName());
        configMap.put(DatawaveSecurityRealm.Config.OPTION_OSCP_LEVEL, "OFF");
        configMap.put(DatawaveSecurityRealm.Config.OPTION_ROLE_PROPERTIES, propertiesFile.toAbsolutePath().toString());

        roleProperties.put(USER_1_PRINCIPAL_NAME, "LocalRoleA, , LocalRoleB, LocalRoleC ,");
        roleProperties.put(USER_1_EVIDENCE_NAME, "LocalRoleD, , LocalRoleE,");

        DatawaveSecurityRealm realm = createSecurityRealm();

        // Call getRealmIdentity to trigger initialization.
        realm.getRealmIdentity(new NamePrincipal("username"));

        // Verify the identity cache was added to the cache manager.
        verify(cacheManager).addCache(any());

        // Verify a certificate validator was created.
        assertNotNull(realm.getX509EvidenceValidator());

        // Verify that local roles were loaded, and that only non-blank, trimmed roles were stored.
        UserRoleMap localUserRoles = realm.getLocalUserRoles();
        assertTrue(localUserRoles.get(USER_1_PRINCIPAL_NAME).containsAll(Set.of("LocalRoleA", "LocalRoleB", "LocalRoleC")));
        assertTrue(localUserRoles.get(USER_1_EVIDENCE_NAME).containsAll(Set.of("LocalRoleD", "LocalRoleE")));

        // Verify the identity cache is only ever added to the cache manager once, even on subsequent calls.
        realm.getRealmIdentity(new NamePrincipal("anotheruser"));
        verify(cacheManager, atMostOnce()).addCache(any());
    }

    /**
     * Verify that when {@link DatawaveSecurityRealm#getRealmIdentity(Principal)} is given a non-DatawavePrincipal, a non-existent identity is returned.
     */
    @Test
    void testGetRealmIdentityGivenNonDatawavePrincipal() throws Exception {
        DatawaveSecurityRealm realm = createSecurityRealm();
        RealmIdentity identity = realm.getRealmIdentity(new NamePrincipal("name"));
        assertFalse(identity.exists());
    }

    /**
     * Verify that when {@link DatawaveSecurityRealm#getRealmIdentity(Principal)} is called for a principal that does not have a cached identity, a new identity
     * is returned.
     */
    @Test
    void testCachingOfRealmIdentity() throws Exception {
        DatawaveSecurityRealm realm = createSecurityRealm();
        DatawavePrincipal principal = new DatawavePrincipal(USER_1_PRINCIPAL_NAME);
        RealmIdentity identity1 = realm.getRealmIdentity(principal);
        RealmIdentity identity2 = realm.getRealmIdentity(principal);
        assertSame(identity1, identity2);
    }

    /**
     * Verify that when a non {@link datawave.security.evidence.DatawaveEvidence} is supplied to {@link RealmIdentity#verifyEvidence(Evidence)}, false is
     * returned.
     */
    @Test
    void testVerifyEvidenceGivenNonDatawaveEvidence() throws Exception {
        DatawaveSecurityRealm realm = createSecurityRealm();
        DatawavePrincipal principal = new DatawavePrincipal(USER_1_PRINCIPAL_NAME);
        RealmIdentity realmIdentity = realm.getRealmIdentity(principal);

        assertFalse(realmIdentity.verifyEvidence(new PasswordGuessEvidence("password".toCharArray())));
        assertTrue(realmIdentity.exists());
    }

    /**
     * Verify that when no X509 certificate verifier is configured for the realm, {@link RealmIdentity#verifyEvidence(Evidence)} returns true when given a
     * {@link X509CertificateEvidence}.
     */
    @Test
    void testVerifyEvidenceGivenX509CertificateEvidenceWithNoVerifierConfigured() throws Exception {
        DatawaveSecurityRealm realm = createSecurityRealm();
        DatawavePrincipal principal = new DatawavePrincipal(USER_1_PRINCIPAL_NAME);
        RealmIdentity realmIdentity = realm.getRealmIdentity(principal);

        assertTrue(realmIdentity.verifyEvidence(new X509CertificateEvidence(USER_1_PRINCIPAL_NAME, List.of(), null)));
        assertTrue(realmIdentity.exists());
    }

    /**
     * Verify that when a X509 certificate verifier is configured for the realm, {@link RealmIdentity#verifyEvidence(Evidence)} returns false when given a
     * {@link X509CertificateEvidence} with an invalid certificate.
     */
    @Test
    void testVerifyEvidenceGivenX509CertificateEvidenceThatFailsVerification() throws Exception {
        configMap.put(DatawaveSecurityRealm.Config.OPTION_CERT_VERIFIER, FailingX509Verifier.class.getName());

        DatawaveSecurityRealm realm = createSecurityRealm();
        DatawavePrincipal principal = new DatawavePrincipal(USER_1_PRINCIPAL_NAME);
        RealmIdentity realmIdentity = realm.getRealmIdentity(principal);

        assertFalse(realmIdentity.verifyEvidence(new X509CertificateEvidence(USER_1_PRINCIPAL_NAME, List.of(), null)));
        assertTrue(realmIdentity.exists());
    }

    /**
     * Verify that when a X509 certificate verifier is configured for the realm, {@link RealmIdentity#verifyEvidence(Evidence)} returns true when given a
     * {@link X509CertificateEvidence} with a valid certificate.
     */
    @Test
    void testVerifyEvidenceGivenX509CertificateEvidenceThatPassesVerification() throws Exception {
        configMap.put(DatawaveSecurityRealm.Config.OPTION_CERT_VERIFIER, PassingX509Verifier.class.getName());

        DatawaveSecurityRealm realm = createSecurityRealm();
        DatawavePrincipal principal = new DatawavePrincipal(USER_1_PRINCIPAL_NAME);
        RealmIdentity realmIdentity = realm.getRealmIdentity(principal);

        X509Certificate certificate = Mockito.mock(X509Certificate.class);
        when(certificate.getIssuerX500Principal()).thenReturn(new X500Principal("CN=TestIssuer"));

        assertTrue(realmIdentity.verifyEvidence(new X509CertificateEvidence(USER_1_PRINCIPAL_NAME, List.of(), certificate)));
        assertTrue(realmIdentity.exists());
    }

    /**
     * Verify that when no local roles are found for the principal or evidence, they are not added to the primary user roles.
     */
    @Test
    void testAttributesWithNoLocalRolesForPrincipalAndNoLocalRolesForEvidence() throws Exception {
        configMap.put(DatawaveSecurityRealm.Config.OPTION_ROLE_PROPERTIES, propertiesFile.toAbsolutePath().toString());

        // Add local roles for some usernames.
        roleProperties.put(USER_1_PRINCIPAL_NAME, "LocalRoleA, , LocalRoleB, LocalRoleC ,");
        roleProperties.put(USER_1_EVIDENCE_NAME, "LocalRoleD, , LocalRoleE,");

        List<DatawaveUser> users = new ArrayList<>();
        users.add(createUser("cn=otheruser", DatawaveUser.UserType.USER, "RoleA", "RoleB"));
        users.add(createUser("cn=proxyserver1", DatawaveUser.UserType.SERVER, "RoleC", "RoleD"));
        users.add(createUser("cn=proxyserver2", DatawaveUser.UserType.SERVER, "RoleE", "RoleF"));

        DatawavePrincipal principal = new DatawavePrincipal(users);
        DatawaveSecurityRealm realm = createSecurityRealm();

        // Verify the realm identity principal is a name principal of the datawave principal's name.
        RealmIdentity identity = realm.getRealmIdentity(principal);
        assertEquals(identity.getRealmIdentityPrincipal(), new NamePrincipal(principal.getName()));

        // Verify the initial attributes.
        Attributes attributes = identity.getAttributes();
        assertEquals(principal.getName(), attributes.getLast(ATTRIBUTE_USERNAME));
        assertThat(attributes.get(AttributeConstants.ATTRIBUTE_PRIMARY_USER_ROLES)).containsExactlyInAnyOrder("RoleA", "RoleB");
        assertThat(attributes.get(AttributeConstants.ATTRIBUTE_TERMINAL_SERVER_ROLES)).containsExactlyInAnyOrder("RoleE", "RoleF");
        assertThat(attributes.get(AttributeConstants.ATTRIBUTE_PROXIED_USER_KEYS)).containsExactlyInAnyOrder("PROXIED_USER_0", "PROXIED_USER_1",
                        "PROXIED_USER_2");
        assertThat(attributes.get("PROXIED_USER_0")).containsExactlyInAnyOrder("RoleA", "RoleB");
        assertThat(attributes.get("PROXIED_USER_1")).containsExactlyInAnyOrder("RoleC", "RoleD");
        assertThat(attributes.get("PROXIED_USER_2")).containsExactlyInAnyOrder("RoleE", "RoleF");

        // Pass in evidence where the evidence username will match against local roles loaded in via a properties.
        TrustedHeaderEvidence trustedHeaderEvidence = new TrustedHeaderEvidence("cn=otheruser<cn=testissuer>", List.of());
        identity.verifyEvidence(trustedHeaderEvidence);

        // Verify the attribute post-verification. It should be the same.
        Attributes postVerificationAttributes = identity.getAttributes();
        assertEquals(principal.getName(), postVerificationAttributes.getLast(ATTRIBUTE_USERNAME));
        assertThat(postVerificationAttributes.get(AttributeConstants.ATTRIBUTE_PRIMARY_USER_ROLES)).containsExactlyInAnyOrder("RoleA", "RoleB");
        assertThat(postVerificationAttributes.get(AttributeConstants.ATTRIBUTE_TERMINAL_SERVER_ROLES)).containsExactlyInAnyOrder("RoleE", "RoleF");
        assertThat(postVerificationAttributes.get(AttributeConstants.ATTRIBUTE_PROXIED_USER_KEYS)).containsExactlyInAnyOrder("PROXIED_USER_0", "PROXIED_USER_1",
                        "PROXIED_USER_2");
        assertThat(postVerificationAttributes.get("PROXIED_USER_0")).containsExactlyInAnyOrder("RoleA", "RoleB");
        assertThat(postVerificationAttributes.get("PROXIED_USER_1")).containsExactlyInAnyOrder("RoleC", "RoleD");
        assertThat(postVerificationAttributes.get("PROXIED_USER_2")).containsExactlyInAnyOrder("RoleE", "RoleF");
    }

    /**
     * Verify that when no local roles are found for the principal, they are not added to the primary user roles, but when local roles are found for evidence,
     * they are added after verification.
     */
    @Test
    void testAttributesWithNoLocalRolesForPrincipalAndLocalRolesForEvidence() throws Exception {
        configMap.put(DatawaveSecurityRealm.Config.OPTION_ROLE_PROPERTIES, propertiesFile.toAbsolutePath().toString());

        // Add local roles for the evidence username.
        roleProperties.put(USER_1_EVIDENCE_NAME, "LocalRoleD, , LocalRoleE,");

        List<DatawaveUser> users = new ArrayList<>();
        users.add(createUser("cn=otheruser", DatawaveUser.UserType.USER, "RoleA", "RoleB"));
        users.add(createUser("cn=proxyserver1", DatawaveUser.UserType.SERVER, "RoleC", "RoleD"));
        users.add(createUser("cn=proxyserver2", DatawaveUser.UserType.SERVER, "RoleE", "RoleF"));

        DatawavePrincipal principal = new DatawavePrincipal(users);
        DatawaveSecurityRealm realm = createSecurityRealm();

        // Verify the realm identity principal is a name principal of the datawave principal's name.
        RealmIdentity identity = realm.getRealmIdentity(principal);
        assertEquals(identity.getRealmIdentityPrincipal(), new NamePrincipal(principal.getName()));

        // Verify the initial attributes.
        Attributes attributes = identity.getAttributes();
        assertEquals(principal.getName(), attributes.getLast(ATTRIBUTE_USERNAME));
        assertThat(attributes.get(AttributeConstants.ATTRIBUTE_PRIMARY_USER_ROLES)).containsExactlyInAnyOrder("RoleA", "RoleB");
        assertThat(attributes.get(AttributeConstants.ATTRIBUTE_TERMINAL_SERVER_ROLES)).containsExactlyInAnyOrder("RoleE", "RoleF");
        assertThat(attributes.get(AttributeConstants.ATTRIBUTE_PROXIED_USER_KEYS)).containsExactlyInAnyOrder("PROXIED_USER_0", "PROXIED_USER_1",
                        "PROXIED_USER_2");
        assertThat(attributes.get("PROXIED_USER_0")).containsExactlyInAnyOrder("RoleA", "RoleB");
        assertThat(attributes.get("PROXIED_USER_1")).containsExactlyInAnyOrder("RoleC", "RoleD");
        assertThat(attributes.get("PROXIED_USER_2")).containsExactlyInAnyOrder("RoleE", "RoleF");

        // Pass in evidence where the evidence username will match against local roles loaded in via a properties.
        TrustedHeaderEvidence trustedHeaderEvidence = new TrustedHeaderEvidence(USER_1_EVIDENCE_NAME, List.of());
        identity.verifyEvidence(trustedHeaderEvidence);

        // Verify the attribute post-verification.
        Attributes postVerificationAttributes = identity.getAttributes();
        assertEquals(principal.getName(), postVerificationAttributes.getLast(ATTRIBUTE_USERNAME));
        // The primary user roles attribute should now also have the local roles matching the evidence username.
        assertThat(postVerificationAttributes.get(AttributeConstants.ATTRIBUTE_PRIMARY_USER_ROLES)).containsExactlyInAnyOrder("RoleA", "RoleB", "LocalRoleE",
                        "LocalRoleD");
        assertThat(postVerificationAttributes.get(AttributeConstants.ATTRIBUTE_TERMINAL_SERVER_ROLES)).containsExactlyInAnyOrder("RoleE", "RoleF");
        assertThat(postVerificationAttributes.get(AttributeConstants.ATTRIBUTE_PROXIED_USER_KEYS)).containsExactlyInAnyOrder("PROXIED_USER_0", "PROXIED_USER_1",
                        "PROXIED_USER_2");
        assertThat(postVerificationAttributes.get("PROXIED_USER_0")).containsExactlyInAnyOrder("RoleA", "RoleB");
        assertThat(postVerificationAttributes.get("PROXIED_USER_1")).containsExactlyInAnyOrder("RoleC", "RoleD");
        assertThat(postVerificationAttributes.get("PROXIED_USER_2")).containsExactlyInAnyOrder("RoleE", "RoleF");
    }

    /**
     * Verify that when local roles are found for the principal, they are added to the primary user roles. Additionally, when no local roles are found for
     * evidence, the attributes are not modified post-verification.
     */
    @Test
    void testAttributesWithLocalRolesForPrincipalAndNoLocalRolesForEvidence() throws Exception {
        configMap.put(DatawaveSecurityRealm.Config.OPTION_ROLE_PROPERTIES, propertiesFile.toAbsolutePath().toString());

        List<DatawaveUser> users = new ArrayList<>();
        users.add(createUser(USER_1_PRINCIPAL_NAME, DatawaveUser.UserType.USER, "RoleA", "RoleB"));
        users.add(createUser("cn=proxyserver1", DatawaveUser.UserType.SERVER, "RoleC", "RoleD"));
        users.add(createUser("cn=proxyserver2", DatawaveUser.UserType.SERVER, "RoleE", "RoleF"));

        DatawavePrincipal principal = new DatawavePrincipal(users);

        // Add local roles for the principal name.
        roleProperties.put(principal.getName(), "LocalRoleA, , LocalRoleB, LocalRoleC ,");

        DatawaveSecurityRealm realm = createSecurityRealm();

        // Verify the realm identity principal is a name principal of the datawave principal's name.
        RealmIdentity identity = realm.getRealmIdentity(principal);
        assertEquals(identity.getRealmIdentityPrincipal(), new NamePrincipal(principal.getName()));

        // Verify the initial attributes.
        Attributes attributes = identity.getAttributes();
        assertEquals(principal.getName(), attributes.getLast(ATTRIBUTE_USERNAME));
        assertThat(attributes.get(AttributeConstants.ATTRIBUTE_PRIMARY_USER_ROLES)).containsExactlyInAnyOrder("RoleA", "RoleB", "LocalRoleA", "LocalRoleB",
                        "LocalRoleC");
        assertThat(attributes.get(AttributeConstants.ATTRIBUTE_TERMINAL_SERVER_ROLES)).containsExactlyInAnyOrder("RoleE", "RoleF");
        assertThat(attributes.get(AttributeConstants.ATTRIBUTE_PROXIED_USER_KEYS)).containsExactlyInAnyOrder("PROXIED_USER_0", "PROXIED_USER_1",
                        "PROXIED_USER_2");
        assertThat(attributes.get("PROXIED_USER_0")).containsExactlyInAnyOrder("RoleA", "RoleB");
        assertThat(attributes.get("PROXIED_USER_1")).containsExactlyInAnyOrder("RoleC", "RoleD");
        assertThat(attributes.get("PROXIED_USER_2")).containsExactlyInAnyOrder("RoleE", "RoleF");

        // Pass in evidence where the evidence username will not match against local roles loaded in via a properties.
        TrustedHeaderEvidence trustedHeaderEvidence = new TrustedHeaderEvidence("cn=otheruser<cn=testissuer>", List.of());
        identity.verifyEvidence(trustedHeaderEvidence);

        // Verify the attributes post-verification. They should be unchanged.
        Attributes postVerificationAttributes = identity.getAttributes();
        assertEquals(principal.getName(), postVerificationAttributes.getLast(ATTRIBUTE_USERNAME));
        assertThat(postVerificationAttributes.get(AttributeConstants.ATTRIBUTE_PRIMARY_USER_ROLES)).containsExactlyInAnyOrder("RoleA", "RoleB", "LocalRoleA",
                        "LocalRoleB", "LocalRoleC");
        assertThat(postVerificationAttributes.get(AttributeConstants.ATTRIBUTE_TERMINAL_SERVER_ROLES)).containsExactlyInAnyOrder("RoleE", "RoleF");
        assertThat(postVerificationAttributes.get(AttributeConstants.ATTRIBUTE_PROXIED_USER_KEYS)).containsExactlyInAnyOrder("PROXIED_USER_0", "PROXIED_USER_1",
                        "PROXIED_USER_2");
        assertThat(postVerificationAttributes.get("PROXIED_USER_0")).containsExactlyInAnyOrder("RoleA", "RoleB");
        assertThat(postVerificationAttributes.get("PROXIED_USER_1")).containsExactlyInAnyOrder("RoleC", "RoleD");
        assertThat(postVerificationAttributes.get("PROXIED_USER_2")).containsExactlyInAnyOrder("RoleE", "RoleF");
    }

    /**
     * Verify that when local roles are found for the principal, they are added to the primary user roles. Additionally, when local roles are found for the
     * evidence, they are added post-verification.
     */
    @Test
    void testAttributesWithLocalRolesForPrincipalAndLocalRolesForEvidence() throws Exception {
        configMap.put(DatawaveSecurityRealm.Config.OPTION_ROLE_PROPERTIES, propertiesFile.toAbsolutePath().toString());

        List<DatawaveUser> users = new ArrayList<>();
        users.add(createUser(USER_1_PRINCIPAL_NAME, DatawaveUser.UserType.USER, "RoleA", "RoleB"));
        users.add(createUser("cn=proxyserver1", DatawaveUser.UserType.SERVER, "RoleC", "RoleD"));
        users.add(createUser("cn=proxyserver2", DatawaveUser.UserType.SERVER, "RoleE", "RoleF"));

        DatawavePrincipal principal = new DatawavePrincipal(users);

        // Add local roles for the principal name, and the evidence username.
        roleProperties.put(principal.getName(), "LocalRoleA, , LocalRoleB, LocalRoleC ,");
        roleProperties.put(USER_1_EVIDENCE_NAME, "LocalRoleD, , LocalRoleE,");

        DatawaveSecurityRealm realm = createSecurityRealm();

        // Verify the realm identity principal is a name principal of the datawave principal's name.
        RealmIdentity identity = realm.getRealmIdentity(principal);
        assertEquals(identity.getRealmIdentityPrincipal(), new NamePrincipal(principal.getName()));

        // Verify the initial attributes.
        Attributes attributes = identity.getAttributes();
        assertEquals(principal.getName(), attributes.getLast(ATTRIBUTE_USERNAME));
        assertThat(attributes.get(AttributeConstants.ATTRIBUTE_PRIMARY_USER_ROLES)).containsExactlyInAnyOrder("RoleA", "RoleB", "LocalRoleA", "LocalRoleB",
                        "LocalRoleC");
        assertThat(attributes.get(AttributeConstants.ATTRIBUTE_TERMINAL_SERVER_ROLES)).containsExactlyInAnyOrder("RoleE", "RoleF");
        assertThat(attributes.get(AttributeConstants.ATTRIBUTE_PROXIED_USER_KEYS)).containsExactlyInAnyOrder("PROXIED_USER_0", "PROXIED_USER_1",
                        "PROXIED_USER_2");
        assertThat(attributes.get("PROXIED_USER_0")).containsExactlyInAnyOrder("RoleA", "RoleB");
        assertThat(attributes.get("PROXIED_USER_1")).containsExactlyInAnyOrder("RoleC", "RoleD");
        assertThat(attributes.get("PROXIED_USER_2")).containsExactlyInAnyOrder("RoleE", "RoleF");

        // Pass in evidence where the evidence username will match against local roles loaded in via a properties.
        TrustedHeaderEvidence trustedHeaderEvidence = new TrustedHeaderEvidence(USER_1_EVIDENCE_NAME, List.of());
        identity.verifyEvidence(trustedHeaderEvidence);

        // Verify the attributes post-verification.
        Attributes postVerificationAttributes = identity.getAttributes();
        assertEquals(principal.getName(), postVerificationAttributes.getLast(ATTRIBUTE_USERNAME));
        // The primary user roles attribute should now also have the local roles matching the evidence username.
        assertThat(postVerificationAttributes.get(AttributeConstants.ATTRIBUTE_PRIMARY_USER_ROLES)).containsExactlyInAnyOrder("RoleA", "RoleB", "LocalRoleA",
                        "LocalRoleB", "LocalRoleC", "LocalRoleD", "LocalRoleE");
        assertThat(postVerificationAttributes.get(AttributeConstants.ATTRIBUTE_TERMINAL_SERVER_ROLES)).containsExactlyInAnyOrder("RoleE", "RoleF");
        assertThat(postVerificationAttributes.get(AttributeConstants.ATTRIBUTE_PROXIED_USER_KEYS)).containsExactlyInAnyOrder("PROXIED_USER_0", "PROXIED_USER_1",
                        "PROXIED_USER_2");
        assertThat(postVerificationAttributes.get("PROXIED_USER_0")).containsExactlyInAnyOrder("RoleA", "RoleB");
        assertThat(postVerificationAttributes.get("PROXIED_USER_1")).containsExactlyInAnyOrder("RoleC", "RoleD");
        assertThat(postVerificationAttributes.get("PROXIED_USER_2")).containsExactlyInAnyOrder("RoleE", "RoleF");
    }

    public DatawaveSecurityRealm createSecurityRealm() throws IOException {
        roleProperties.store(Files.newBufferedWriter(propertiesFile), null);
        DatawaveSecurityRealm realm = new DatawaveSecurityRealm();
        realm.setSecurityEJBProvider(securityEJBProvider);
        realm.initialize(configMap);
        return realm;
    }

    private DatawaveUser createUser(String subjectDn, DatawaveUser.UserType userType, String... roles) {
        SubjectIssuerDNPair dnPair = SubjectIssuerDNPair.of(subjectDn, ISSUER_DN);
        return new DatawaveUser(dnPair, userType, auths, Set.of(roles), ImmutableMultimap.of(), System.currentTimeMillis());
    }

    public static class FailingX509Verifier implements X509CertificateVerifier {

        @Override
        public boolean verify(X509Certificate cert, String alias, KeyStore keyStore, KeyStore trustStore) {
            return false;
        }
    }

    public static class PassingX509Verifier implements X509CertificateVerifier {

        @Override
        public boolean verify(X509Certificate cert, String alias, KeyStore keyStore, KeyStore trustStore) {
            return true;
        }
    }
}
