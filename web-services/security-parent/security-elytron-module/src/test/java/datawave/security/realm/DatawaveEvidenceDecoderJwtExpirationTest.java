package datawave.security.realm;

import static datawave.security.realm.DatawaveEvidenceDecoder.Config.OPTION_JWT_ENABLED;
import static datawave.security.realm.DatawaveEvidenceDecoder.Config.OPTION_MAX_CACHE_AGE;
import static datawave.security.realm.DatawaveEvidenceDecoder.Config.OPTION_MAX_CACHE_ENTRIES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Principal;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wildfly.security.evidence.Evidence;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.cache.ElytronCacheManager;
import datawave.security.evidence.JWTEvidence;
import datawave.security.system.SecurityEJBProvider;
import io.jsonwebtoken.ExpiredJwtException;

/**
 * Demonstrates that {@link DatawaveEvidenceDecoder} does not re-validate a JWT once the users decoded from it have been cached.
 * <p>
 * {@link DatawaveEvidenceDecoder#getPrincipal(Evidence)} keys its user cache on the raw token and consults that cache <em>before</em> delegating to
 * {@link DatawaveUserProvider#getUsers(Evidence)}, which is the only path that reaches {@link JWTTokenHandler#createUsersFromToken(String)} where the signature
 * and {@code exp} claim are checked. On a cache hit that validation is skipped entirely, so an expired token continues to authenticate for as long as its cache
 * entry lives.
 * <p>
 * Nothing downstream compensates: {@link DatawaveUser#getExpirationTime()} is never consulted on the authentication path, so the {@code exp} claim is the only
 * expiry enforcement that exists.
 * <p>
 * NOTE: {@link #testExpiredJwtStillAuthenticatesOnCacheHit()} is a characterization test. It asserts the current, incorrect behavior so that the defect is
 * reproducible, which means it passes only while the defect is present. Whoever fixes the decoder should invert that test to assert the expired token is
 * rejected, rather than delete it.
 */
class DatawaveEvidenceDecoderJwtExpirationTest {

    private static final String TEST_SUBJECT = "cn=testuser";
    private static final String TEST_ISSUER = "cn=testissuer";
    private static final String EXPECTED_PRINCIPAL_NAME = TEST_SUBJECT + "<" + TEST_ISSUER + ">";

    private static final Set<String> auths = Set.of("A", "B", "C");
    private static final Set<String> roles = Set.of("Administrator", "InternalUser");
    private static final Multimap<String,String> rolesToAuths = ImmutableMultimap.of("Administrator", "A", "Administrator", "B", "Administrator", "C",
                    "InternalUser", "A");

    /**
     * The lifetime of the JWTs minted by this test. The {@code exp} claim has one-second granularity, so this must be comfortably greater than one second for
     * the token to be reliably valid at the time of the first decode.
     */
    private static final long TOKEN_TTL_MILLIS = 3000L;

    /**
     * The user cache TTL, set to the value the production Wildfly configuration intends so that the cache clearly outlives the token.
     */
    private static final long CACHE_TTL_MILLIS = 300_000L;

    /**
     * How long to wait for a minted token to actually expire before giving up.
     */
    private static final long EXPIRY_TIMEOUT_MILLIS = 30_000L;

    private static final long POLL_INTERVAL_MILLIS = 100L;

    private static final Clock clock = Clock.systemUTC();

    private static X509Certificate certificate;
    private static KeyPair keyPair;

    private JWTTokenHandler tokenHandler;

    @BeforeAll
    static void beforeAll() throws Exception {
        Security.addProvider(new BouncyCastleProvider());

        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA", "BC");
        generator.initialize(2048);
        keyPair = generator.generateKeyPair();

        X500Name subjectDn = new X500Name("CN=certUser");
        X500Name issuerDn = new X500Name("CN=certIssuer");
        BigInteger serialNumber = BigInteger.valueOf(new SecureRandom().nextInt());
        Date validFrom = new Date();
        Date validTo = new Date(validFrom.getTime() + 365 * 25 * 60 * 60 * 1000L);

        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(issuerDn, serialNumber, validFrom, validTo, subjectDn, keyPair.getPublic());
        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").setProvider("BC").build(keyPair.getPrivate());
        certificate = new JcaX509CertificateConverter().setProvider("BC").getCertificate(builder.build(signer));
    }

    @BeforeEach
    void setUp() {
        // @formatter:off
        ObjectMapper mapper = JsonMapper.builder()
                        .enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME)
                        .build()
                        .registerModules(new GuavaModule())
                        .registerModules(new JaxbAnnotationModule());
        // @formatter:on

        tokenHandler = new JWTTokenHandler(certificate, keyPair.getPrivate(), TOKEN_TTL_MILLIS, TimeUnit.MILLISECONDS,
                        JWTTokenHandler.TtlMode.RELATIVE_TO_CURRENT_TIME, mapper);
    }

    /**
     * Verify that a JWT which authenticated while it was valid continues to authenticate after it has expired, because the users decoded from it were cached
     * against the raw token and the cache is consulted before the token is ever re-parsed.
     */
    @Test
    void testExpiredJwtStillAuthenticatesOnCacheHit() throws Exception {
        DatawaveEvidenceDecoder evidenceDecoder = createDecoder(CACHE_TTL_MILLIS);

        String token = tokenHandler.createTokenFromUsers(EXPECTED_PRINCIPAL_NAME, Set.of(createUser()));

        // The short-lived JWT authenticates while it is still valid.
        Principal principal = evidenceDecoder.getPrincipal(new JWTEvidence(token));
        assertNotNull(principal, "The valid JWT should have authenticated");
        assertEquals(EXPECTED_PRINCIPAL_NAME, principal.getName());

        // Wait until the JWT has genuinely expired, proven by the token handler rejecting it outright.
        awaitTokenExpiration(token);

        // The same, now-expired, JWT still authenticates because the decoder answers from its cache without re-parsing the token.
        Principal principalAfterExpiry = evidenceDecoder.getPrincipal(new JWTEvidence(token));
        assertNotNull(principalAfterExpiry, "BUG: the expired JWT was still accepted and authenticated from the cache");
        assertEquals(EXPECTED_PRINCIPAL_NAME, principalAfterExpiry.getName(),
                        "BUG: the expired JWT resolved to a fully populated principal with no re-validation");
    }

    /**
     * Verify that the same expired JWT <em>is</em> rejected when its cache entry has already expired, confirming that the token is validated on a cache miss
     * and that the bypass in {@link #testExpiredJwtStillAuthenticatesOnCacheHit()} is caused solely by the cache hit.
     * <p>
     * This also shows that lowering the cache TTL only bounds the window in which an expired token remains usable; it does not close it.
     */
    @Test
    void testExpiredJwtIsRejectedOnCacheMiss() throws Exception {
        // Use a cache TTL short enough that the cached users are gone well before the token itself expires.
        DatawaveEvidenceDecoder evidenceDecoder = createDecoder(1L);

        String token = tokenHandler.createTokenFromUsers(EXPECTED_PRINCIPAL_NAME, Set.of(createUser()));

        Principal principal = evidenceDecoder.getPrincipal(new JWTEvidence(token));
        assertNotNull(principal, "The valid JWT should have authenticated");

        awaitTokenExpiration(token);

        // With no usable cache entry the decoder re-parses the token, and the expired token is correctly rejected.
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> evidenceDecoder.getPrincipal(new JWTEvidence(token)),
                        "The expired JWT should have been rejected when it was not served from the cache");
        assertEquals(ExpiredJwtException.class, thrown.getCause().getClass(), "The rejection should have been caused by the expired exp claim");
    }

    /**
     * Block until the given token is rejected as expired by the token handler, which is the same call the decoder bypasses on a cache hit.
     *
     * @param token
     *            the token to wait on
     * @throws InterruptedException
     *             if interrupted while waiting
     */
    private void awaitTokenExpiration(String token) throws InterruptedException {
        long deadline = clock.millis() + EXPIRY_TIMEOUT_MILLIS;
        while (clock.millis() < deadline) {
            try {
                tokenHandler.createUsersFromToken(token);
            } catch (ExpiredJwtException e) {
                return;
            }
            Thread.sleep(POLL_INTERVAL_MILLIS);
        }
        fail("The JWT did not expire within " + EXPIRY_TIMEOUT_MILLIS + "ms");
    }

    /**
     * Create a decoder wired to a real {@link DatawaveUserProvider} and {@link JWTTokenHandler} so that JWT validation is genuinely exercised.
     *
     * @param maxCacheAge
     *            the user cache TTL in milliseconds
     * @return the decoder
     */
    private DatawaveEvidenceDecoder createDecoder(long maxCacheAge) {
        ElytronCacheManager cacheManager = mock(ElytronCacheManager.class);
        SecurityEJBProvider securityEJBProvider = mock(SecurityEJBProvider.class);
        when(securityEJBProvider.getElytronCacheManager()).thenReturn(cacheManager);

        DatawaveUserProvider userProvider = new DatawaveUserProvider(mock(DatawaveUserService.class), tokenHandler);

        DatawaveEvidenceDecoder evidenceDecoder = new DatawaveEvidenceDecoder();
        evidenceDecoder.setDatawaveUserProvider(userProvider);
        evidenceDecoder.setSecurityEJBProvider(securityEJBProvider);
        evidenceDecoder.initialize(Map.of(OPTION_JWT_ENABLED, "true", OPTION_MAX_CACHE_ENTRIES, "-1", OPTION_MAX_CACHE_AGE, String.valueOf(maxCacheAge)));
        return evidenceDecoder;
    }

    private DatawaveUser createUser() {
        SubjectIssuerDNPair dnPair = SubjectIssuerDNPair.of(TEST_SUBJECT, TEST_ISSUER);
        return new DatawaveUser(dnPair, DatawaveUser.UserType.USER, auths, roles, rolesToAuths, clock.millis());
    }
}
