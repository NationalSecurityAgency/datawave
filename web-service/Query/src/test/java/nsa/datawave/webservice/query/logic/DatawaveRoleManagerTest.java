package nsa.datawave.webservice.query.logic;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nsa.datawave.security.authorization.DatawavePrincipal;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DatawaveRoleManagerTest {
    
    private DatawaveRoleManager drm;
    private DatawavePrincipal datawavePrincipal;
    private Principal p;
    
    @Before
    public void beforeEachTest() {
        createAndSetWithSingleRole();
    }
    
    private void createAndSetWithSingleRole() {
        
        String dn = "dn1";
        String issuerDN = "idn";
        String combinedDN = dn + "<" + issuerDN + ">";
        
        String[] authArray = new String[] {"dn1", "dn2"};
        datawavePrincipal = new DatawavePrincipal(authArray);
        List<String> roles = new ArrayList<>();
        String[] authsForRoles = new String[] {"auth1", "auth2", "auth3"};
        
        Collections.addAll(roles, authsForRoles);
        datawavePrincipal.setUserRoles(combinedDN, roles);
        
        Map<String,Collection<String>> roleMap = new LinkedHashMap<>();
        Set<String> datawaveRoles = new HashSet<String>();
        datawaveRoles.add("REQ_ROLE_1");
        roleMap.put(combinedDN, datawaveRoles);
        
        datawavePrincipal.setUserRoles(combinedDN, datawaveRoles);
    }
    
    private void createAndSetWithTwoRoles() {
        
        String dn = "dn1";
        String issuerDN = "idn";
        String combinedDN = dn + "<" + issuerDN + ">";
        
        String[] authArray = new String[] {dn, dn};
        datawavePrincipal = new DatawavePrincipal(authArray);
        List<String> roles = new ArrayList<>();
        String[] authsForRoles = new String[] {"auth1", "auth2", "auth3"};
        
        Collections.addAll(roles, authsForRoles);
        
        String dn2 = "dn2";
        String combinedDN2 = dn2 + "<" + issuerDN + ">";
        
        // Set the user DN to have multiple roles
        datawavePrincipal.setUserRoles(combinedDN, getFirstRole());
        datawavePrincipal.setUserRoles(combinedDN2, getSecondRole());
    }
    
    public Set<String> getFirstRole() {
        Set<String> datawaveRoles = new HashSet<String>();
        datawaveRoles.add("REQ_ROLE_1");
        return datawaveRoles;
    }
    
    public Set<String> getSecondRole() {
        Set<String> datawaveRoles = new HashSet<String>();
        datawaveRoles.add("REQ_ROLE_2");
        return datawaveRoles;
    }
    
    public Set<String> getAllRoles() {
        Set<String> datawaveRoles = new HashSet<String>();
        datawaveRoles.add("REQ_ROLE_1");
        datawaveRoles.add("REQ_ROLE_2");
        return datawaveRoles;
    }
    
    @Test
    public void testEmptyConstructor() {
        
        drm = new DatawaveRoleManager();
        
        Set<String> gottenRoles = drm.getRequiredRoles();
        Assert.assertEquals(null, gottenRoles);
        
        drm.setRequiredRoles(getFirstRole());
        gottenRoles = drm.getRequiredRoles();
        
        Assert.assertEquals(true, gottenRoles.contains("REQ_ROLE_1"));
        Assert.assertEquals(false, gottenRoles.contains("REQ_ROLE_2"));
    }
    
    @Test
    public void testBasicsLoadedConstructor() {
        
        drm = new DatawaveRoleManager(getFirstRole());
        
        Set<String> gottenRoles = drm.getRequiredRoles();
        Assert.assertEquals(true, gottenRoles.contains("REQ_ROLE_1"));
        Assert.assertEquals(false, gottenRoles.contains("REQ_ROLE_2"));
    }
    
    @Test
    public void testCanRunQuery() {
        
        drm = new DatawaveRoleManager(getFirstRole());
        
        // Expect false when passing in a null Principal object
        boolean canRun = drm.canRunQuery(null, null);
        Assert.assertEquals(false, canRun);
        
        // Modify the principal and set the required roles to null
        p = datawavePrincipal;
        Assert.assertNotEquals(null, p);
        drm.setRequiredRoles(null);
        
        // This test should pass when setting requiredRoles to null
        canRun = drm.canRunQuery(null, p);
        Assert.assertEquals(true, canRun);
        
        // Now set up a test that requires roles to run
        drm.setRequiredRoles(getFirstRole());
        canRun = drm.canRunQuery(null, p);
        Assert.assertEquals(true, canRun);
        
        // Now add a second required role check
        drm.setRequiredRoles(getAllRoles());
        canRun = drm.canRunQuery(null, p);
        Assert.assertEquals(false, canRun);
        
        // Recreate the principal with two roles and check
        createAndSetWithTwoRoles();
        p = datawavePrincipal;
        drm.setRequiredRoles(getFirstRole());
        canRun = drm.canRunQuery(null, p);
        Assert.assertEquals(true, canRun);
    }
}
