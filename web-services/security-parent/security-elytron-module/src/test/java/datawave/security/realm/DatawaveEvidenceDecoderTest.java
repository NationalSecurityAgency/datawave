package datawave.security.realm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wildfly.security.evidence.Evidence;
import org.wildfly.security.evidence.PasswordGuessEvidence;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.cache.ElytronCacheManager;
import datawave.security.evidence.JWTEvidence;
import datawave.security.evidence.TrustedHeaderEvidence;
import datawave.security.evidence.X509CertificateEvidence;
import datawave.security.system.SecurityEJBProvider;

class DatawaveEvidenceDecoderTest {

    private static final String TEST_SUBJECT = "cn=testuser";
    private static final String TEST_ISSUER = "cn=testissuer";
    private static final Set<String> auths = Set.of("A", "B", "C");
    private static final Set<String> roles = Set.of("Administrator", "InternalUser");
    private static final Multimap<String,String> rolesToAuths = ImmutableMultimap.of("Administrator", "A", "Administrator", "B", "Administrator", "C",
                    "InternalUser", "A");

    private final Map<String,String> configMap = new HashMap<>();
    private ElytronCacheManager cacheManager;
    private SecurityEJBProvider securityEJBProvider;
    private DatawaveUserProvider datawaveUserProvider;
    private DatawaveEvidenceDecoder evidenceDecoder;

    @BeforeEach
    void setUp() {
        evidenceDecoder = null;
        configMap.clear();
        cacheManager = mock(ElytronCacheManager.class);
        securityEJBProvider = mock(SecurityEJBProvider.class);
        when(securityEJBProvider.getElytronCacheManager()).thenReturn(cacheManager);
        datawaveUserProvider = mock(DatawaveUserProvider.class);
    }

    /**
     * Verify that when {@link DatawaveEvidenceDecoder#getPrincipal(Evidence)} is called, initialization is complete and the user cache is added to the elytron
     * cache manager exactly once.
     */
    @Test
    void verifyUserCacheAddedToElytronCacheManager() {
        decode(new PasswordGuessEvidence("password".toCharArray()));
        verify(cacheManager, atLeastOnce()).addCache(any());

        // Verify the cache is not added to the manager again on subsequent calls.
        decode(new PasswordGuessEvidence("password".toCharArray()));
        decode(new PasswordGuessEvidence("password".toCharArray()));
        verify(cacheManager, atMostOnce()).addCache(any());
    }

    /**
     * Verify that {@link DatawaveEvidenceDecoder#getPrincipal(Evidence)} returns null given evidence that is not an instance of
     * {@link datawave.security.evidence.DatawaveEvidence}.
     */
    @Test
    void testNonDatawaveEvidence() {
        assertNull(decode(new PasswordGuessEvidence("password".toCharArray())));
    }

    /**
     * Verify that {@link DatawaveEvidenceDecoder#getPrincipal(Evidence)} returns null given a {@link JWTEvidence} when JWT is disabled.
     */
    @Test
    void testJwtEvidenceGivenJwtDisabled() {
        configMap.put(DatawaveEvidenceDecoder.Config.OPTION_JWT_ENABLED, "false");

        assertNull(decode(new JWTEvidence("token")));
    }

    /**
     * Verify that {@link DatawaveEvidenceDecoder#getPrincipal(Evidence)} returns a {@link DatawavePrincipal} given a {@link JWTEvidence} when JWT is enabled.
     */
    @Test
    void testJwtEvidenceGivenJwtEnabled() throws Exception {
        configMap.put(DatawaveEvidenceDecoder.Config.OPTION_JWT_ENABLED, "true");

        JWTEvidence evidence = new JWTEvidence("token");

        when(datawaveUserProvider.getUsers(evidence)).thenReturn(Set.of(createUser()));

        DatawavePrincipal principal = (DatawavePrincipal) decode(evidence);

        assertEquals(TEST_SUBJECT + "<" + TEST_ISSUER + ">", principal.getName());
    }

    /**
     * Verify that {@link DatawaveEvidenceDecoder#getPrincipal(Evidence)} returns null given a {@link TrustedHeaderEvidence} when trusted header authentication
     * is disabled.
     */
    @Test
    void testTrustedHeaderEvidenceGivenTrustedHeadersDisabled() {
        configMap.put(DatawaveEvidenceDecoder.Config.OPTION_TRUSTED_HEADERS_ENABLED, "false");

        assertNull(decode(new TrustedHeaderEvidence(TEST_SUBJECT + "<" + TEST_ISSUER + ">", List.of(SubjectIssuerDNPair.of(TEST_SUBJECT, TEST_ISSUER)))));
    }

    /**
     * Verify that {@link DatawaveEvidenceDecoder#getPrincipal(Evidence)} returns a {@link DatawavePrincipal} given a {@link TrustedHeaderEvidence} when trusted
     * header authentication is enabled.
     */
    @Test
    void testTrustedHeaderEvidenceGivenTrustedHeadersEnabled() throws Exception {
        configMap.put(DatawaveEvidenceDecoder.Config.OPTION_TRUSTED_HEADERS_ENABLED, "true");

        TrustedHeaderEvidence evidence = new TrustedHeaderEvidence(TEST_SUBJECT + "<" + TEST_ISSUER + ">",
                        List.of(SubjectIssuerDNPair.of(TEST_SUBJECT, TEST_ISSUER)));

        when(datawaveUserProvider.getUsers(evidence)).thenReturn(Set.of(createUser()));

        DatawavePrincipal principal = (DatawavePrincipal) decode(evidence);

        assertEquals(TEST_SUBJECT + "<" + TEST_ISSUER + ">", principal.getName());
    }

    /**
     * Verify that {@link DatawaveEvidenceDecoder#getPrincipal(Evidence)} returns a {@link DatawavePrincipal} given a {@link X509CertificateEvidence} when
     * trusted header authentication is enabled.
     */
    @Test
    void testX509CertificateEvidence() throws Exception {
        X509CertificateEvidence evidence = new X509CertificateEvidence(TEST_SUBJECT + "<" + TEST_ISSUER + ">",
                        List.of(SubjectIssuerDNPair.of(TEST_SUBJECT, TEST_ISSUER)), null);

        when(datawaveUserProvider.getUsers(evidence)).thenReturn(Set.of(createUser()));

        DatawavePrincipal principal = (DatawavePrincipal) decode(evidence);

        assertEquals(TEST_SUBJECT + "<" + TEST_ISSUER + ">", principal.getName());
    }

    /**
     * Verify that when {@link DatawaveEvidenceDecoder#getPrincipal(Evidence)} is called multiple times for the same evidence, that the user cache is used after
     * the first call to fetch the users.
     */
    @Test
    void testCaching() throws Exception {
        configMap.put(DatawaveEvidenceDecoder.Config.OPTION_TRUSTED_HEADERS_ENABLED, "true");

        TrustedHeaderEvidence evidence = new TrustedHeaderEvidence(TEST_SUBJECT + "<" + TEST_ISSUER + ">",
                        List.of(SubjectIssuerDNPair.of(TEST_SUBJECT, TEST_ISSUER)));

        when(datawaveUserProvider.getUsers(evidence)).thenReturn(Set.of(createUser()));

        assertNotNull(decode(evidence));
        assertNotNull(decode(evidence));
        assertNotNull(decode(evidence));

        // The users should have been fetch from the provider only once, and then the cache should have been used for the subsequent calls.
        verify(datawaveUserProvider, atMostOnce()).getUsers(evidence);
    }

    /**
     * Verify that users for {@link JWTEvidence} are not cached.
     */
    @Test
    void testJWTEvidenceIsNotCached() throws Exception {
        configMap.put(DatawaveEvidenceDecoder.Config.OPTION_JWT_ENABLED, "true");

        JWTEvidence evidence = new JWTEvidence("token");
        when(datawaveUserProvider.getUsers(evidence)).thenReturn(Set.of(createUser()));

        assertNotNull(decode(evidence));
        assertNotNull(decode(evidence));
        assertNotNull(decode(evidence));

        // The users should have been fetch from the provider only once, and then the cache should have been used for the subsequent calls.
        verify(datawaveUserProvider, atLeast(3)).getUsers(evidence);
    }

    private Principal decode(Evidence evidence) {
        // Initialize the decoder if needed.
        if (evidenceDecoder == null) {
            evidenceDecoder = new DatawaveEvidenceDecoder();
            evidenceDecoder.setDatawaveUserProvider(datawaveUserProvider);
            evidenceDecoder.setSecurityEJBProvider(securityEJBProvider);
            evidenceDecoder.initialize(configMap);
        }

        return evidenceDecoder.getPrincipal(evidence);
    }

    private DatawaveUser createUser() {
        SubjectIssuerDNPair dnPair = SubjectIssuerDNPair.of(TEST_SUBJECT, TEST_ISSUER);
        return new DatawaveUser(dnPair, DatawaveUser.UserType.USER, auths, roles, rolesToAuths, System.currentTimeMillis());
    }
}
