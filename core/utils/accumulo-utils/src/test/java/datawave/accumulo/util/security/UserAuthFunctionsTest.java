package datawave.accumulo.util.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;

public class UserAuthFunctionsTest {
    
    private static final UserAuthFunctions UAF = new UserAuthFunctions.Default();
    
    private static final String USER_DN = "userDN";
    private static final String ISSUER_DN = "issuerDN";
    private String requestedAuths;
    private HashSet<Set<String>> userAuths;
    
    private DatawaveUser user;
    private DatawaveUser p1;
    private DatawaveUser p2;
    private Collection<DatawaveUser> proxyChain;
    
    @BeforeEach
    public void initialize() {
        requestedAuths = "A,C";
        userAuths = new HashSet<>();
        userAuths.add(Sets.newHashSet("A", "C", "D"));
        userAuths.add(Sets.newHashSet("A", "B", "E"));
        
        SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of(USER_DN, ISSUER_DN);
        SubjectIssuerDNPair p1dn = SubjectIssuerDNPair.of("entity1UserDN", "entity1IssuerDN");
        SubjectIssuerDNPair p2dn = SubjectIssuerDNPair.of("entity2UserDN", "entity2IssuerDN");
        
        user = new DatawaveUser(userDN, DatawaveUser.UserType.USER, Sets.newHashSet("A", "C", "D"), null, null, System.currentTimeMillis());
        p1 = new DatawaveUser(p1dn, DatawaveUser.UserType.SERVER, Sets.newHashSet("A", "B", "E"), null, null, System.currentTimeMillis());
        p2 = new DatawaveUser(p2dn, DatawaveUser.UserType.SERVER, Sets.newHashSet("A", "F", "G"), null, null, System.currentTimeMillis());
        proxyChain = Lists.newArrayList(user, p1, p2);
    }
    
    @Test
    public void testDowngradeAuthorizations() {
        HashSet<Authorizations> expected = Sets.newHashSet(new Authorizations("A", "C"), new Authorizations("A", "B", "E"), new Authorizations("A", "F", "G"));
        assertEquals(expected, UAF.mergeAuthorizations(UAF.getRequestedAuthorizations(requestedAuths, user), proxyChain, u -> u != user));
    }
    
    @Test
    public void testDowngradeAuthorizations2() {
        HashSet<Authorizations> expected = Sets.newHashSet(new Authorizations("A", "C"), new Authorizations("A", "B", "E"), new Authorizations("A", "F", "G"));
        assertEquals(expected, UAF.mergeAuthorizations(UAF.getRequestedAuthorizations(requestedAuths, user, true), proxyChain, u -> u != user));
    }
    
    @Test
    public void testUserRequestsAuthTheyDontHave() {
        requestedAuths = "A,C,D,X,Y,Z";
        assertThrows(IllegalArgumentException.class, () -> UAF.getRequestedAuthorizations(requestedAuths, user));
    }
    
    @Test
    public void testUserRequestsAuthTheyDontHave2() {
        requestedAuths = "A,C,D,X,Y,Z";
        assertThrows(IllegalArgumentException.class, () -> UAF.getRequestedAuthorizations(requestedAuths, user, true));
    }
    
    @Test
    public void testUserRequestsAuthTheyDontHaveNoThrow() {
        requestedAuths = "A,C,D,X,Y,Z";
        Authorizations expected = new Authorizations("A", "C", "D");
        assertEquals(expected, UAF.getRequestedAuthorizations(requestedAuths, user, false));
    }
    
    @Test
    public void testValidateUserRequestsAuth() throws AuthorizationException {
        requestedAuths = "A,C";
        UAF.validateRequestedAuthorizations(requestedAuths, user);
    }
    
    @Test
    public void testValidateUserRequestsAuthTheyDontHave() {
        requestedAuths = "A,C,D,X,Y,Z";
        assertThrows(AuthorizationException.class, () -> UAF.validateRequestedAuthorizations(requestedAuths, user));
    }
    
    @Test
    public void testUserAuthsFirstInMergedSet() {
        HashSet<Authorizations> mergedAuths = UAF.mergeAuthorizations(UAF.getRequestedAuthorizations(requestedAuths, user), proxyChain, u -> u != user);
        assertEquals(3, mergedAuths.size());
        assertEquals(new Authorizations("A", "C"), mergedAuths.iterator().next(), "Merged user authorizations were not first in the return set");
    }
    
    @Test
    public void testUserIsNull() {
        assertEquals(Authorizations.EMPTY, UAF.getRequestedAuthorizations(requestedAuths, null));
    }
    
    @Test
    public void testRequestedAuthsIsNull() {
        assertEquals(new Authorizations("A", "C", "D"), UAF.getRequestedAuthorizations(null, user));
        assertEquals(new Authorizations("A", "C", "D"), UAF.getRequestedAuthorizations("", user));
    }
}
