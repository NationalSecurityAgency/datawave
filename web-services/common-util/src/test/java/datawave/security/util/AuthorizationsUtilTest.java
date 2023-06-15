package datawave.security.util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils.NpeUtils;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AuthorizationsUtilTest {
    private static final String USER_DN = "userDN";
    private static final String ISSUER_DN = "issuerDN";
    private String methodAuths;
    private String remoteAuths;
    private HashSet<Set<String>> userAuths;
    private DatawavePrincipal proxiedUserPrincipal;
    private DatawavePrincipal proxiedServerPrincipal1;
    private DatawavePrincipal proxiedServerPrincipal2;
    private DatawavePrincipal remoteUserPrincipal;
    // the overall user is a combination of the proxied and remote users
    private DatawavePrincipal overallUserPrincipal;

    @Before
    public void initialize() {
        System.setProperty(NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        methodAuths = "A,C";
        userAuths = new HashSet<>();
        userAuths.add(Sets.newHashSet("A", "C", "D"));
        userAuths.add(Sets.newHashSet("A", "B", "E"));

        SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of(USER_DN, ISSUER_DN);
        SubjectIssuerDNPair p1dn = SubjectIssuerDNPair.of("entity1UserDN", "entity1IssuerDN");
        SubjectIssuerDNPair p2dn = SubjectIssuerDNPair.of("entity2UserDN", "entity2IssuerDN");
        SubjectIssuerDNPair p3dn = SubjectIssuerDNPair.of("entity3UserDN", "entity3IssuerDN");

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, Sets.newHashSet("A", "C", "D"), null, null, System.currentTimeMillis());
        DatawaveUser p1 = new DatawaveUser(p1dn, UserType.SERVER, Sets.newHashSet("A", "B", "E"), null, null, System.currentTimeMillis());
        DatawaveUser p2 = new DatawaveUser(p2dn, UserType.SERVER, Sets.newHashSet("A", "F", "G"), null, null, System.currentTimeMillis());
        DatawaveUser p3 = new DatawaveUser(p3dn, UserType.SERVER, Sets.newHashSet("A", "B", "G"), null, null, System.currentTimeMillis());

        proxiedUserPrincipal = new DatawavePrincipal(Lists.newArrayList(user, p1, p2));
        proxiedServerPrincipal1 = new DatawavePrincipal(Lists.newArrayList(p3, p1));
        proxiedServerPrincipal2 = new DatawavePrincipal(Lists.newArrayList(p2, p3, p1));

        DatawaveUser user_2 = new DatawaveUser(userDN, UserType.USER, Sets.newHashSet("A", "D", "E", "H"), null, null, System.currentTimeMillis());
        remoteUserPrincipal = new DatawavePrincipal(Lists.newArrayList(user_2, p1, p2));
        remoteAuths = "A,E";

        DatawaveUser overallUser = new DatawaveUser(userDN, UserType.USER, Sets.newHashSet("A", "C", "D", "E", "H"), null, null, System.currentTimeMillis());

        overallUserPrincipal = new DatawavePrincipal(Lists.newArrayList(overallUser, p1, p2));
    }

    @Test
    public void testMergeAuthorizations() {
        HashSet<Authorizations> expected = Sets.newHashSet(new Authorizations("A", "C"), new Authorizations("A"));
        assertEquals(expected, AuthorizationsUtil.mergeAuthorizations(methodAuths, userAuths));
    }

    @Test
    public void testDowngradeAuthorizations() throws AuthorizationException {
        HashSet<Authorizations> expected = Sets.newHashSet(new Authorizations("A", "C"), new Authorizations("A", "B", "E"), new Authorizations("A", "F", "G"));
        assertEquals(expected, AuthorizationsUtil.getDowngradedAuthorizations(methodAuths, proxiedUserPrincipal, proxiedUserPrincipal));
    }

    @Test(expected = AuthorizationException.class)
    public void testDowngradeAuthorizationsUserRequestsAuthTheyDontHave() throws AuthorizationException {
        AuthorizationsUtil.getDowngradedAuthorizations("A,C,E", proxiedUserPrincipal, proxiedUserPrincipal);
        fail("Exception not thrown!");
    }

    @Test(expected = AuthorizationException.class)
    public void testDowngradeAuthorizationsServerRequestsAuthTheyDontHave1() throws AuthorizationException {
        // p1, p3 - call will succeed if p1 is primaryUser, throw exception if p3 is primaryUser
        AuthorizationsUtil.getDowngradedAuthorizations("A,B,E", proxiedServerPrincipal1, proxiedServerPrincipal1);
        fail("Exception not thrown!");
    }

    @Test(expected = AuthorizationException.class)
    public void testDowngradeAuthorizationsServerRequestsAuthTheyDontHave2() throws AuthorizationException {
        // p1, p2, p3 - call will succeed if p1 is primaryUser, throw exception if p2 is primaryUser
        AuthorizationsUtil.getDowngradedAuthorizations("A,B,E", proxiedServerPrincipal2, proxiedServerPrincipal2);
        fail("Exception not thrown!");
    }

    @Test
    public void testDowngradeRemoteAuthorizations() throws AuthorizationException {
        HashSet<Authorizations> expected = Sets.newHashSet(new Authorizations("A", "E"), new Authorizations("A", "B", "E"), new Authorizations("A", "F", "G"));
        assertEquals(expected, AuthorizationsUtil.getDowngradedAuthorizations(remoteAuths, overallUserPrincipal, remoteUserPrincipal));
    }

    @Test(expected = AuthorizationException.class)
    public void testDowngradeRemoteAuthorizationsFail() throws AuthorizationException {
        HashSet<Authorizations> expected = Sets.newHashSet(new Authorizations("A"), new Authorizations("A", "B", "E"), new Authorizations("A", "F", "E"));
        assertEquals(expected, AuthorizationsUtil.getDowngradedAuthorizations(methodAuths, remoteUserPrincipal, remoteUserPrincipal));
    }

    @Test
    public void testUserAuthsFirstInMergedSet() throws AuthorizationException {
        Set<Authorizations> mergedAuths = AuthorizationsUtil.getDowngradedAuthorizations(methodAuths, proxiedUserPrincipal, proxiedUserPrincipal);
        assertEquals(3, mergedAuths.size());
        assertEquals("Merged user authorizations were not first in the return set", new Authorizations("A", "C"), mergedAuths.iterator().next());
    }

    @Test
    public void testUnionAuthorizations() {
        assertEquals(new Authorizations("A", "C"), AuthorizationsUtil.union(new Authorizations("A", "C"), new Authorizations("A")));
    }

    @Test
    public void testUnionWithEmptyAuthorizations() {
        assertEquals(new Authorizations("A", "C"), AuthorizationsUtil.union(new Authorizations("A", "C"), new Authorizations()));
    }

    @Test
    public void testUnionWithBothEmptyAuthorizations() {
        assertEquals(new Authorizations(), AuthorizationsUtil.union(new Authorizations(), new Authorizations()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUserRequestsAuthTheyDontHave() {
        // This is the case where we could throw an error or write something to the logs
        String methodAuths = "A,C,F";
        AuthorizationsUtil.mergeAuthorizations(methodAuths, userAuths);
        fail("Exception not thrown!");
    }

    @Test
    public void testMethodAuthsIsNull() {
        HashSet<Authorizations> expected = new HashSet<>();
        for (Set<String> auths : userAuths) {
            expected.add(new Authorizations(auths.toArray(new String[auths.size()])));
        }
        assertEquals(expected, AuthorizationsUtil.mergeAuthorizations(null, userAuths));
    }

    @Test
    public void testUserAuthsIsNull() {
        assertEquals(Collections.singleton(new Authorizations()), AuthorizationsUtil.mergeAuthorizations(methodAuths, null));
    }

    @Test
    public void testBothMethodAndUserAuthsNull() {
        assertEquals(Collections.singleton(new Authorizations()), AuthorizationsUtil.mergeAuthorizations(null, null));
    }

    @Test
    public void testMinimizeWithSubset() {
        ArrayList<Authorizations> authSets = Lists.newArrayList(new Authorizations("A", "B", "C", "D"), new Authorizations("C", "B"),
                        new Authorizations("A", "B", "C"), new Authorizations("B", "C", "D", "E"));
        Collection<Authorizations> expected = Collections.singleton(new Authorizations("B", "C"));

        assertEquals(expected, AuthorizationsUtil.minimize(authSets));
    }

    @Test
    public void testMinimizeWithNoSubset() {
        LinkedHashSet<Authorizations> expected = new LinkedHashSet<>();
        expected.add(new Authorizations("A", "B", "C", "D"));
        expected.add(new Authorizations("B", "C", "F"));
        expected.add(new Authorizations("A", "B", "E"));
        expected.add(new Authorizations("B", "C", "D", "E"));

        assertEquals(expected, AuthorizationsUtil.minimize(expected));
    }

    @Test
    public void testMinimizeWithMultipleSubsets() {
        LinkedHashSet<Authorizations> testSet = new LinkedHashSet<>();
        testSet.add(new Authorizations("A", "B", "C", "D"));
        testSet.add(new Authorizations("B", "C"));
        testSet.add(new Authorizations("A", "B", "E"));
        testSet.add(new Authorizations("A", "B", "D", "E"));

        LinkedHashSet<Authorizations> expected = new LinkedHashSet<>();
        expected.add(new Authorizations("B", "C"));
        expected.add(new Authorizations("A", "B", "E"));

        assertEquals(expected, AuthorizationsUtil.minimize(testSet));
    }

    @Test
    public void testMinimizeWithDupsButNoSubset() {
        ArrayList<Authorizations> authSets = Lists.newArrayList(new Authorizations("A", "B", "C", "D"), new Authorizations("B", "C", "F"),
                        new Authorizations("A", "B", "C", "D"), new Authorizations("B", "C", "D", "E"));

        LinkedHashSet<Authorizations> expected = new LinkedHashSet<>();
        expected.add(new Authorizations("A", "B", "C", "D"));
        expected.add(new Authorizations("B", "C", "F"));
        expected.add(new Authorizations("B", "C", "D", "E"));
        assertEquals(expected, AuthorizationsUtil.minimize(authSets));
    }

    @Test
    public void testBuilidAuthorizationString() {
        Collection<Collection<String>> auths = new HashSet<>();
        List<String> authsList = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "A", "E", "I", "J");

        HashSet<String> uniqAuths = new HashSet<>(authsList);

        auths.add(authsList.subList(0, 4));
        auths.add(authsList.subList(4, 8));
        auths.add(authsList.subList(8, 12));
        uniqAuths.removeAll(Arrays.asList(AuthorizationsUtil.buildAuthorizationString(auths).split(",")));
        assertTrue(uniqAuths.isEmpty());
    }

    @Test
    public void testBuildUserAuthorizationsString() throws Exception {
        String expected = new Authorizations("A", "C", "D").toString();
        assertEquals(expected, AuthorizationsUtil.buildUserAuthorizationString(proxiedUserPrincipal));
    }

    @Test
    public void testMergeUsers() {
        SubjectIssuerDNPair userDn1 = SubjectIssuerDNPair.of("entity1UserDN", "entity1IssuerDN");
        SubjectIssuerDNPair userDn2 = SubjectIssuerDNPair.of("entity1UserDN", "entity1IssuerDN");

        DatawaveUser user1 = new DatawaveUser(userDn1, UserType.USER, Sets.newHashSet("A", "C", "D"), null, null, System.currentTimeMillis());
        DatawaveUser user2 = new DatawaveUser(userDn2, UserType.USER, Sets.newHashSet("A", "B", "E"), null, null, System.currentTimeMillis());

        DatawaveUser user3 = AuthorizationsUtil.mergeUsers(user1, user2);

        DatawaveUser expected = new DatawaveUser(userDn1, user1.getUserType(), Sets.newHashSet("A", "B", "C", "D", "E"), null, null, -1);
        assertUserEquals(expected, user3);

        Multimap<String,String> rolesToAuths1 = HashMultimap.create();
        rolesToAuths1.put("role1", "A");
        rolesToAuths1.put("role1", "B");
        rolesToAuths1.put("role2", "C");
        Multimap<String,String> rolesToAuths2 = HashMultimap.create();
        rolesToAuths2.put("role3", "A");
        Multimap<String,String> rolesToAuths3 = HashMultimap.create(rolesToAuths1);
        rolesToAuths3.putAll(rolesToAuths2);

        user1 = new DatawaveUser(userDn1, UserType.USER, Sets.newHashSet("A", "C", "D"), rolesToAuths1.keySet(), rolesToAuths1, System.currentTimeMillis());
        user2 = new DatawaveUser(userDn2, UserType.USER, Sets.newHashSet("A", "B", "E"), rolesToAuths2.keySet(), rolesToAuths2, System.currentTimeMillis());

        user3 = AuthorizationsUtil.mergeUsers(user1, user2);

        expected = new DatawaveUser(userDn1, UserType.USER, Sets.newHashSet("A", "B", "C", "D", "E"), rolesToAuths3.keySet(), rolesToAuths3, -1);
        assertUserEquals(expected, user3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotMergeUser() {
        AuthorizationsUtil.mergeUsers(proxiedServerPrincipal1.getPrimaryUser(), proxiedServerPrincipal2.getPrimaryUser());
    }

    @Test
    public void testMergePrincipals() {
        DatawavePrincipal merged = AuthorizationsUtil.mergePrincipals(proxiedUserPrincipal, remoteUserPrincipal);
        assertPrincipalEquals(overallUserPrincipal, merged);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCannotMergePrincipal() {
        AuthorizationsUtil.mergePrincipals(proxiedServerPrincipal1, proxiedServerPrincipal2);
    }

    private void assertUserEquals(DatawaveUser user1, DatawaveUser user2) {
        assertEquals(user1.getDn(), user2.getDn());
        assertEquals(user1.getUserType(), user2.getUserType());
        assertEquals(new HashSet<>(user1.getAuths()), new HashSet<>(user2.getAuths()));
        assertEquals(new HashSet<>(user1.getRoles()), new HashSet<>(user2.getRoles()));
        assertEquals(user1.getRoleToAuthMapping(), user2.getRoleToAuthMapping());
    }

    private void assertPrincipalEquals(DatawavePrincipal user1, DatawavePrincipal user2) {
        List<DatawaveUser> users1 = new ArrayList<>(user1.getProxiedUsers());
        List<DatawaveUser> users2 = new ArrayList<>(user2.getProxiedUsers());
        assertEquals(users1.size(), users2.size());
        for (int i = 0; i < users1.size(); i++) {
            assertUserEquals(users1.get(i), users2.get(i));
        }
    }
}
