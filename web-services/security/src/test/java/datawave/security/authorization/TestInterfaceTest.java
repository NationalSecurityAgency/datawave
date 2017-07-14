package datawave.security.authorization;

import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(EasyMockRunner.class)
public class TestInterfaceTest extends EasyMockSupport {
    private TestAuthorizationService testAuthorizationService;
    
    @Before
    public void setUp() throws Exception {
        replayAll();
        
        testAuthorizationService = new TestAuthorizationService();
        HashMap<String,List<String>> roleMap = new HashMap<>();
        roleMap.put("testUser", Arrays.asList("Role1", "Role2", "Role3"));
        roleMap.put("testUser2", null);
        Whitebox.setInternalState(testAuthorizationService, Map.class, roleMap);
        
        verifyAll();
        resetAll();
    }
    
    @Test
    public void testValidUser() throws Exception {
        String userDN = "testUser";
        String issuerDN = "testIssuer";
        String projectName = "testProject";
        
        replayAll();
        
        String[] roles = testAuthorizationService.getRoles(projectName, userDN, issuerDN);
        assertArrayEquals(new String[] {"Role1", "Role2", "Role3"}, roles);
        
        verifyAll();
    }
    
    @Test
    public void testInvalidUser() throws Exception {
        String userDN = "dummyUser";
        String issuerDN = "testIssuer";
        String projectName = "testProject";
        
        replayAll();
        
        String[] roles = testAuthorizationService.getRoles(projectName, userDN, issuerDN);
        assertNull(roles);
        
        verifyAll();
    }
    
    @Test
    public void testNullRoleList() throws Exception {
        String userDN = "testUser2";
        String issuerDN = "testIssuer";
        String projectName = "testProject";
        
        replayAll();
        
        String[] roles = testAuthorizationService.getRoles(projectName, userDN, issuerDN);
        assertNull(roles);
        
        verifyAll();
    }
    
    @Test
    public void testNullRoleMap() throws Exception {
        String userDN = "testUser";
        String issuerDN = "testIssuer";
        String projectName = "testProject";
        
        Whitebox.setInternalState(testAuthorizationService, Map.class, (Map) null);
        
        replayAll();
        
        String[] roles = testAuthorizationService.getRoles(projectName, userDN, issuerDN);
        assertNull(roles);
        
        verifyAll();
    }
}
