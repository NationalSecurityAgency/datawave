package datawave.security.realm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.wildfly.security.evidence.Evidence;
import org.wildfly.security.evidence.PasswordGuessEvidence;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.evidence.JWTEvidence;
import datawave.security.evidence.TrustedHeaderEvidence;
import datawave.security.evidence.X509CertificateEvidence;

/**
 * Tests for {@link DatawaveUserProvider}.
 */
class DatawaveUserProviderTest {

    private static final String TEST_SUBJECT = "cn=testuser";
    private static final String TEST_ISSUER = "cn=testissuer";
    private static final Set<String> auths = Set.of("A", "B", "C");
    private static final Set<String> roles = Set.of("Administrator", "InternalUser");
    private static final Multimap<String,String> rolesToAuths = ImmutableMultimap.of("Administrator", "A", "Administrator", "B", "Administrator", "C",
                    "InternalUser", "A");

    private DatawaveUserService userService;
    private JWTTokenHandler tokenHandler;
    private DatawaveUserProvider userProvider;

    @BeforeEach
    void setUp() {
        userService = Mockito.mock(DatawaveUserService.class);
        tokenHandler = Mockito.mock(JWTTokenHandler.class);
        userProvider = new DatawaveUserProvider(userService, tokenHandler);
    }

    /**
     * Verify that {@link DatawaveUserProvider#getUsers(Evidence)} returns an empty set when given evidence that is not an instance of
     * {@link datawave.security.evidence.DatawaveEvidence}.
     */
    @Test
    void testGetUsersGivenNonDatawaveEvidence() throws Exception {
        Evidence evidence = new PasswordGuessEvidence("password".toCharArray());
        assertTrue(userProvider.getUsers(evidence).isEmpty());
    }

    /**
     * Verify that when {@link DatawaveUserProvider#getUsers(Evidence)} is given a {@link JWTEvidence}, that the token handler is used to look up the users.
     */
    @Test
    void testGetUsersGivenJWTEvidence() throws Exception {
        Collection<DatawaveUser> expected = Set.of(createUser());
        when(tokenHandler.createUsersFromToken("token")).thenReturn(expected);

        JWTEvidence evidence = new JWTEvidence("token");
        Collection<DatawaveUser> actual = userProvider.getUsers(evidence);
        assertEquals(expected, actual);

        verify(tokenHandler).createUsersFromToken("token");
    }

    /**
     * Verify that when {@link DatawaveUserProvider#getUsers(Evidence)} is given a {@link TrustedHeaderEvidence}, that the user service is used to look up the
     * users.
     */
    @Test
    void testGetUsersGivenTrustedHeaderEvidence() throws Exception {
        List<SubjectIssuerDNPair> entities = List.of(SubjectIssuerDNPair.of(TEST_SUBJECT, TEST_ISSUER));
        Collection<DatawaveUser> expected = Set.of(createUser());
        when(userService.lookup(entities)).thenReturn(expected);

        TrustedHeaderEvidence evidence = new TrustedHeaderEvidence(TEST_SUBJECT + "<" + TEST_ISSUER + ">", entities);
        Collection<DatawaveUser> actual = userProvider.getUsers(evidence);
        assertEquals(expected, actual);

        verify(userService).lookup(entities);
    }

    /**
     * Verify that when {@link DatawaveUserProvider#getUsers(Evidence)} is given a {@link TrustedHeaderEvidence}, that the user service is used to look up the
     * users.
     */
    @Test
    void testGetUsersGivenX509CertificateEvidence() throws Exception {
        List<SubjectIssuerDNPair> entities = List.of(SubjectIssuerDNPair.of(TEST_SUBJECT, TEST_ISSUER));
        Collection<DatawaveUser> expected = Set.of(createUser());
        when(userService.lookup(entities)).thenReturn(expected);

        X509CertificateEvidence evidence = new X509CertificateEvidence(TEST_SUBJECT + "<" + TEST_ISSUER + ">", entities, null);
        Collection<DatawaveUser> actual = userProvider.getUsers(evidence);
        assertEquals(expected, actual);

        verify(userService).lookup(entities);
    }

    private DatawaveUser createUser() {
        SubjectIssuerDNPair dnPair = SubjectIssuerDNPair.of(TEST_SUBJECT, TEST_ISSUER);
        return new DatawaveUser(dnPair, DatawaveUser.UserType.USER, auths, roles, rolesToAuths, System.currentTimeMillis());
    }
}
