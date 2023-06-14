package datawave.security.authorization;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;

import datawave.security.authorization.DatawaveUser.UserType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class DatawavePrincipalTest {

    private DatawaveUser finalServer;
    private DatawaveUser server1;
    private DatawaveUser server2;
    private DatawaveUser server3;
    private DatawaveUser user;

    final private String finalConnectionServerSubjectDn = "cn=finalserver";
    final private String server1SubjectDn = "cn=server1";
    final private String server2SubjectDn = "cn=server2";
    final private String server3SubjectDn = "cn=server3";
    final private String userSubjectDn = "cn=user";
    final private String issuerDn = "cn=issuer";

    @Before
    public void setUp() throws Exception {
        long now = System.currentTimeMillis();
        SubjectIssuerDNPair finalConnectionServerDn = SubjectIssuerDNPair.of(finalConnectionServerSubjectDn, issuerDn);
        SubjectIssuerDNPair server1Dn = SubjectIssuerDNPair.of(server1SubjectDn, issuerDn);
        SubjectIssuerDNPair server2Dn = SubjectIssuerDNPair.of(server2SubjectDn, issuerDn);
        SubjectIssuerDNPair server3Dn = SubjectIssuerDNPair.of(server3SubjectDn, issuerDn);
        SubjectIssuerDNPair userDn = SubjectIssuerDNPair.of(userSubjectDn, issuerDn);
        finalServer = new DatawaveUser(finalConnectionServerDn, UserType.SERVER, null, null, null, now);
        server1 = new DatawaveUser(server1Dn, UserType.SERVER, null, null, null, now);
        server2 = new DatawaveUser(server2Dn, UserType.SERVER, null, null, null, now);
        server3 = new DatawaveUser(server3Dn, UserType.SERVER, null, null, null, now);
        user = new DatawaveUser(userDn, UserType.USER, null, null, null, now);
    }

    @Test
    public void GetPrimaryUserTest() {
        // direct call from a server
        DatawavePrincipal dp = new DatawavePrincipal(Lists.newArrayList(finalServer));
        Assert.assertEquals(finalConnectionServerSubjectDn, dp.getPrimaryUser().getDn().subjectDN());

        // direct call from a user
        dp = new DatawavePrincipal(Lists.newArrayList(user));
        Assert.assertEquals(userSubjectDn, dp.getPrimaryUser().getDn().subjectDN());

        // call from finalConnectionServer proxying initial caller server1
        dp = new DatawavePrincipal(Lists.newArrayList(server1, finalServer));
        Assert.assertEquals(server1SubjectDn, dp.getPrimaryUser().getDn().subjectDN());

        // call from finalConnectionServer proxying initial caller server1 through server2
        dp = new DatawavePrincipal(Lists.newArrayList(server1, server2, finalServer));
        Assert.assertEquals(server1SubjectDn, dp.getPrimaryUser().getDn().subjectDN());

        // call from finalConnectionServer proxying initial caller server1 through server2 and server3
        dp = new DatawavePrincipal(Lists.newArrayList(server1, server2, server3, finalServer));
        Assert.assertEquals(server1SubjectDn, dp.getPrimaryUser().getDn().subjectDN());

        // these tests are for case where a UserType.USER appears anywhere in the proxiedUsers collection
        dp = new DatawavePrincipal(Lists.newArrayList(user, server1, server2, server3));
        Assert.assertEquals(userSubjectDn, dp.getPrimaryUser().getDn().subjectDN());

        dp = new DatawavePrincipal(Lists.newArrayList(server1, user, server2, server3));
        Assert.assertEquals(userSubjectDn, dp.getPrimaryUser().getDn().subjectDN());

        dp = new DatawavePrincipal(Lists.newArrayList(server1, server2, user, server3));
        Assert.assertEquals(userSubjectDn, dp.getPrimaryUser().getDn().subjectDN());

        dp = new DatawavePrincipal(Lists.newArrayList(server1, server2, server3, user));
        Assert.assertEquals(userSubjectDn, dp.getPrimaryUser().getDn().subjectDN());
    }

    @Test
    public void GetProxyServersTest() {
        // direct call from finalServer
        DatawavePrincipal dp = new DatawavePrincipal(Lists.newArrayList(finalServer));
        Assert.assertEquals(null, dp.getProxyServers());

        // direct call from user
        dp = new DatawavePrincipal(Lists.newArrayList(user));
        Assert.assertEquals(null, dp.getProxyServers());

        // call from finalServer proxying initial caller server1
        dp = new DatawavePrincipal(Lists.newArrayList(server1, finalServer));
        Assert.assertEquals(Arrays.asList(finalConnectionServerSubjectDn), dp.getProxyServers());

        // call from finalServer proxying initial caller server1 through server2
        dp = new DatawavePrincipal(Lists.newArrayList(server1, server2, finalServer));
        Assert.assertEquals(Arrays.asList(server2SubjectDn, finalConnectionServerSubjectDn), dp.getProxyServers());

        // call from finalServer proxying initial caller server1 through server2 and server3
        dp = new DatawavePrincipal(Lists.newArrayList(server1, server2, server3, finalServer));
        Assert.assertEquals(Arrays.asList(server2SubjectDn, server3SubjectDn, finalConnectionServerSubjectDn), dp.getProxyServers());

        // these tests are for cases where a UserType.USER appears anywhere in the proxiedUsers collection

        dp = new DatawavePrincipal(Lists.newArrayList(user, server1, server2, server3));
        Assert.assertEquals(Arrays.asList(server1SubjectDn, server2SubjectDn, server3SubjectDn), dp.getProxyServers());

        dp = new DatawavePrincipal(Lists.newArrayList(server1, user, server2, server3));
        Assert.assertEquals(Arrays.asList(server1SubjectDn, server2SubjectDn, server3SubjectDn), dp.getProxyServers());

        dp = new DatawavePrincipal(Lists.newArrayList(server1, server2, user, server3));
        Assert.assertEquals(Arrays.asList(server1SubjectDn, server2SubjectDn, server3SubjectDn), dp.getProxyServers());

        // this case would be very odd -- call from user proxying initial caller server1 through server2 through server3
        dp = new DatawavePrincipal(Lists.newArrayList(server1, server2, server3, user));
        Assert.assertEquals(Arrays.asList(server1SubjectDn, server2SubjectDn, server3SubjectDn), dp.getProxyServers());
    }

    private String joinNames(Collection<DatawaveUser> datawaveUsers) {
        return datawaveUsers.stream().map(DatawaveUser::getName).collect(Collectors.joining(" -> "));
    }

    @Test
    public void GetNameTest() {
        // direct call from finalServer
        DatawavePrincipal dp = new DatawavePrincipal(Lists.newArrayList(finalServer));
        Assert.assertEquals(joinNames(Lists.newArrayList(finalServer)), dp.getName());

        // direct call from user
        dp = new DatawavePrincipal(Lists.newArrayList(user));
        Assert.assertEquals(joinNames(Lists.newArrayList(user)), dp.getName());

        // call from finalServer proxying initial caller server1
        dp = new DatawavePrincipal(Lists.newArrayList(server1, finalServer));
        Assert.assertEquals(joinNames(Lists.newArrayList(server1, finalServer)), dp.getName());

        // call from finalServer proxying initial caller server1 through server2
        dp = new DatawavePrincipal(Lists.newArrayList(server1, server2, finalServer));
        Assert.assertEquals(joinNames(Lists.newArrayList(server1, server2, finalServer)), dp.getName());

        // call from finalServer proxying initial caller server1 through server2 and server3
        dp = new DatawavePrincipal(Lists.newArrayList(server1, server2, server3, finalServer));
        Assert.assertEquals(joinNames(Lists.newArrayList(server1, server2, server3, finalServer)), dp.getName());

        // these tests are for cases where a UserType.USER appears anywhere in the proxiedUsers collection

        // this first case would be very odd -- call from user proxying initial caller server1 through server2 through server3
        dp = new DatawavePrincipal(Lists.newArrayList(user, server1, server2, server3));
        Assert.assertEquals(joinNames(Lists.newArrayList(user, server1, server2, server3)), dp.getName());

        dp = new DatawavePrincipal(Lists.newArrayList(server1, user, server2, server3));
        Assert.assertEquals(joinNames(Lists.newArrayList(user, server1, server2, server3)), dp.getName());

        dp = new DatawavePrincipal(Lists.newArrayList(server1, server2, user, server3));
        Assert.assertEquals(joinNames(Lists.newArrayList(user, server1, server2, server3)), dp.getName());

        dp = new DatawavePrincipal(Lists.newArrayList(server1, server2, server3, user));
        Assert.assertEquals(joinNames(Lists.newArrayList(user, server1, server2, server3)), dp.getName());
    }

    @Test
    public void OrderProxiedUsers() {

        // call from finalServer proxying initial caller server1
        Assert.assertEquals(Lists.newArrayList(server1, finalServer), DatawavePrincipal.orderProxiedUsers(Lists.newArrayList(server1, finalServer)));

        // call from finalServer proxying initial caller server1 through server2
        Assert.assertEquals(Lists.newArrayList(server1, server2, finalServer),
                        DatawavePrincipal.orderProxiedUsers(Lists.newArrayList(server1, server2, finalServer)));

        // call from finalServer proxying initial caller server1 through server2 and server3
        Assert.assertEquals(Lists.newArrayList(server1, server2, server3, finalServer),
                        DatawavePrincipal.orderProxiedUsers(Lists.newArrayList(server1, server2, server3, finalServer)));

        // these tests are for cases where a UserType.USER appears anywhere in the proxiedUsers collection

        // this first case would be very odd -- call from user proxying initial caller server1 through server2 through server3
        Assert.assertEquals(Lists.newArrayList(user, server1, server2, server3),
                        DatawavePrincipal.orderProxiedUsers(Lists.newArrayList(user, server1, server2, server3)));

        Assert.assertEquals(Lists.newArrayList(user, server1, server2, server3),
                        DatawavePrincipal.orderProxiedUsers(Lists.newArrayList(server1, user, server2, server3)));

        Assert.assertEquals(Lists.newArrayList(user, server1, server2, server3),
                        DatawavePrincipal.orderProxiedUsers(Lists.newArrayList(server1, server2, user, server3)));

        Assert.assertEquals(Lists.newArrayList(user, server1, server2, server3),
                        DatawavePrincipal.orderProxiedUsers(Lists.newArrayList(server1, server2, server3, user)));
    }

    @Test
    public void DuplicateUserPreserved() {
        // check that duplicate users are preserved
        DatawavePrincipal dp = new DatawavePrincipal(Lists.newArrayList(server1, server2, server1));
        Assert.assertEquals(3, dp.getProxiedUsers().size());
        Assert.assertEquals(server1, dp.getProxiedUsers().stream().findFirst().get());
        Assert.assertEquals(server2, dp.getProxiedUsers().stream().skip(1).findFirst().get());
        Assert.assertEquals(server1, dp.getProxiedUsers().stream().skip(2).findFirst().get());
    }
}
