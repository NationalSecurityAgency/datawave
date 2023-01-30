package datawave.security.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.RemoteUserOperations;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils.NpeUtils;
import datawave.user.AuthorizationsListBase;
import datawave.webservice.result.GenericResponse;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.List;

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
        DatawaveUser p1_2 = new DatawaveUser(p1dn, UserType.SERVER, Sets.newHashSet("A", "B", "E"), null, null, System.currentTimeMillis());
        DatawaveUser p2_2 = new DatawaveUser(p2dn, UserType.SERVER, Sets.newHashSet("A", "F", "E"), null, null, System.currentTimeMillis());
        remoteUserPrincipal = new DatawavePrincipal(Lists.newArrayList(user_2, p1_2, p2_2));
        remoteAuths = "A,E";
    }
    
    @Test
    public void testMergeAuthorizations() {
        HashSet<Authorizations> expected = Sets.newHashSet(new Authorizations("A", "C"), new Authorizations("A"));
        assertEquals(expected, AuthorizationsUtil.mergeAuthorizations(methodAuths, userAuths));
    }
    
    @Test
    public void testDowngradeAuthorizations() throws AuthorizationException {
        HashSet<Authorizations> expected = Sets.newHashSet(new Authorizations("A", "C"), new Authorizations("A", "B", "E"), new Authorizations("A", "F", "G"));
        assertEquals(expected, AuthorizationsUtil.getDowngradedAuthorizations(methodAuths, proxiedUserPrincipal, null));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testDowngradeAuthorizationsUserRequestsAuthTheyDontHave() throws AuthorizationException {
        AuthorizationsUtil.getDowngradedAuthorizations("A,C,E", proxiedUserPrincipal, null);
        fail("Exception not thrown!");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testDowngradeAuthorizationsServerRequestsAuthTheyDontHave1() throws AuthorizationException {
        // p1, p3 - call will succeed if p1 is primaryUser, throw exception if p3 is primaryUser
        AuthorizationsUtil.getDowngradedAuthorizations("A,B,E", proxiedServerPrincipal1, null);
        fail("Exception not thrown!");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testDowngradeAuthorizationsServerRequestsAuthTheyDontHave2() throws AuthorizationException {
        // p1, p2, p3 - call will succeed if p1 is primaryUser, throw exception if p2 is primaryUser
        AuthorizationsUtil.getDowngradedAuthorizations("A,B,E", proxiedServerPrincipal2, null);
        fail("Exception not thrown!");
    }
    
    @Test
    public void testDowngradeRemoteAuthorizations() throws AuthorizationException {
        RemoteUserOperations remoteOps = new RemoteUserOperations() {
            
            @Override
            public AuthorizationsListBase listEffectiveAuthorizations(Object callerObject) throws AuthorizationException {
                return null;
            }
            
            @Override
            public GenericResponse<String> flushCachedCredentials(Object callerObject) {
                return null;
            }
            
            @Override
            public DatawavePrincipal getRemoteUser(DatawavePrincipal principal) throws AuthorizationException {
                return remoteUserPrincipal;
            }
        };
        HashSet<Authorizations> expected = Sets.newHashSet(new Authorizations("A", "E"), new Authorizations("A", "B", "E"), new Authorizations("A", "F", "E"));
        assertEquals(expected, AuthorizationsUtil.getDowngradedAuthorizations(remoteAuths, proxiedUserPrincipal, remoteOps));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testDowngradeRemoteAuthorizationsFail() throws AuthorizationException {
        RemoteUserOperations remoteOps = new RemoteUserOperations() {
            
            @Override
            public AuthorizationsListBase listEffectiveAuthorizations(Object callerObject) throws AuthorizationException {
                return null;
            }
            
            @Override
            public GenericResponse<String> flushCachedCredentials(Object callerObject) {
                return null;
            }
            
            @Override
            public DatawavePrincipal getRemoteUser(DatawavePrincipal principal) throws AuthorizationException {
                return remoteUserPrincipal;
            }
        };
        HashSet<Authorizations> expected = Sets.newHashSet(new Authorizations("A"), new Authorizations("A", "B", "E"), new Authorizations("A", "F", "E"));
        assertEquals(expected, AuthorizationsUtil.getDowngradedAuthorizations(methodAuths, proxiedUserPrincipal, remoteOps));
    }
    
    @Test
    public void testUserAuthsFirstInMergedSet() throws AuthorizationException {
        HashSet<Authorizations> mergedAuths = AuthorizationsUtil.getDowngradedAuthorizations(methodAuths, proxiedUserPrincipal, null);
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
        methodAuths = "A,C,F";
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
        ArrayList<Authorizations> authSets = Lists.newArrayList(new Authorizations("A", "B", "C", "D"), new Authorizations("C", "B"), new Authorizations("A",
                        "B", "C"), new Authorizations("B", "C", "D", "E"));
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
        ArrayList<Authorizations> authSets = Lists.newArrayList(new Authorizations("A", "B", "C", "D"), new Authorizations("B", "C", "F"), new Authorizations(
                        "A", "B", "C", "D"), new Authorizations("B", "C", "D", "E"));
        
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
    public void testBuildUserAuthorizationsString() {
        String expected = new Authorizations("A", "C", "D").toString();
        assertEquals(expected, AuthorizationsUtil.buildUserAuthorizationString(proxiedUserPrincipal));
    }
}
