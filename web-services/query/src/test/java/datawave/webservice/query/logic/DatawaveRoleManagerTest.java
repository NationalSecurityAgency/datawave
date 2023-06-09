package datawave.webservice.query.logic;

import java.security.Principal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Lists;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils.NpeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DatawaveRoleManagerTest {

    private DatawaveRoleManager drm;
    private DatawavePrincipal datawavePrincipal;
    private Principal p;

    @Before
    public void beforeEachTest() {
        System.setProperty(NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        createAndSetWithSingleRole();
    }

    private void createAndSetWithSingleRole() {

        String dn = "dn1";
        String issuerDN = "idn";
        SubjectIssuerDNPair combinedDN = SubjectIssuerDNPair.of(dn, issuerDN);
        Collection<String> roles = Lists.newArrayList("REQ_ROLE_1");

        DatawaveUser user = new DatawaveUser(combinedDN, UserType.USER, null, roles, null, System.currentTimeMillis());
        datawavePrincipal = new DatawavePrincipal(Lists.newArrayList(user));
    }

    private void createAndSetWithTwoRoles() {

        String dn = "dn1";
        String issuerDN = "idn";
        SubjectIssuerDNPair combinedDn1 = SubjectIssuerDNPair.of(dn, issuerDN);
        String combinedDN = dn + "<" + issuerDN + ">";
        String dn2 = "dn2";
        String combinedDN2 = dn2 + "<" + issuerDN + ">";
        SubjectIssuerDNPair combinedDn2 = SubjectIssuerDNPair.of(dn2, issuerDN);

        DatawaveUser u1 = new DatawaveUser(combinedDn1, UserType.USER, null, getFirstRole(), null, System.currentTimeMillis());
        DatawaveUser u2 = new DatawaveUser(combinedDn2, UserType.SERVER, null, getSecondRole(), null, System.currentTimeMillis());

        datawavePrincipal = new DatawavePrincipal(Lists.newArrayList(u1, u2));
    }

    public Set<String> getFirstRole() {
        Set<String> datawaveRoles = new HashSet<>();
        datawaveRoles.add("REQ_ROLE_1");
        return datawaveRoles;
    }

    public Set<String> getSecondRole() {
        Set<String> datawaveRoles = new HashSet<>();
        datawaveRoles.add("REQ_ROLE_2");
        return datawaveRoles;
    }

    public Set<String> getAllRoles() {
        Set<String> datawaveRoles = new HashSet<>();
        datawaveRoles.add("REQ_ROLE_1");
        datawaveRoles.add("REQ_ROLE_2");
        return datawaveRoles;
    }

    @Test
    public void testEmptyConstructor() {

        drm = new DatawaveRoleManager();

        Set<String> gottenRoles = drm.getRequiredRoles();
        Assert.assertNull(gottenRoles);

        drm.setRequiredRoles(getFirstRole());
        gottenRoles = drm.getRequiredRoles();

        Assert.assertTrue(gottenRoles.contains("REQ_ROLE_1"));
        Assert.assertFalse(gottenRoles.contains("REQ_ROLE_2"));
    }

    @Test
    public void testBasicsLoadedConstructor() {

        drm = new DatawaveRoleManager(getFirstRole());

        Set<String> gottenRoles = drm.getRequiredRoles();
        Assert.assertTrue(gottenRoles.contains("REQ_ROLE_1"));
        Assert.assertFalse(gottenRoles.contains("REQ_ROLE_2"));
    }

    @Test
    public void testCanRunQuery() {

        drm = new DatawaveRoleManager(getFirstRole());

        // Expect false when passing in a null Principal object
        boolean canRun = drm.canRunQuery(null, null);
        Assert.assertFalse(canRun);

        // Modify the principal and set the required roles to null
        p = datawavePrincipal;
        Assert.assertNotEquals(null, p);
        drm.setRequiredRoles(null);

        // This test should pass when setting requiredRoles to null
        canRun = drm.canRunQuery(null, p);
        Assert.assertTrue(canRun);

        // Now set up a test that requires roles to run
        drm.setRequiredRoles(getFirstRole());
        canRun = drm.canRunQuery(null, p);
        Assert.assertTrue(canRun);

        // Now add a second required role check
        drm.setRequiredRoles(getAllRoles());
        canRun = drm.canRunQuery(null, p);
        Assert.assertFalse(canRun);

        // Recreate the principal with two roles and check
        createAndSetWithTwoRoles();
        p = datawavePrincipal;
        drm.setRequiredRoles(getFirstRole());
        canRun = drm.canRunQuery(null, p);
        Assert.assertTrue(canRun);
    }
}
