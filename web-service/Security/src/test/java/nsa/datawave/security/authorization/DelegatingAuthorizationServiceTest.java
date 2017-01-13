package nsa.datawave.security.authorization;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.*;
import static org.powermock.api.easymock.PowerMock.createStrictMock;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.resetAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DelegatingAuthorizationService.class})
public class DelegatingAuthorizationServiceTest {
    private DelegatingAuthorizationService delegatingAuthorizationInterface;
    private AuthorizationService authorizationInterface1;
    private AuthorizationService authorizationInterface2;
    private AuthorizationService authorizationInterface3;
    
    @Before
    public void setUp() throws Exception {
        
        authorizationInterface1 = createStrictMock(AuthorizationService.class);
        authorizationInterface2 = createStrictMock(AuthorizationService.class);
        authorizationInterface3 = createStrictMock(AuthorizationService.class);
        
        replayAll();
        
        delegatingAuthorizationInterface = new DelegatingAuthorizationService();
        delegatingAuthorizationInterface.setDelegates(Arrays.asList(authorizationInterface1, authorizationInterface2, authorizationInterface3));
        
        verifyAll();
        resetAll();
    }
    
    @Test
    public void testGetRolesAllNull() throws Exception {
        String userDN = "testUser";
        String issuerDN = "testIssuer";
        String projectName = "testProject";
        
        expect(authorizationInterface1.getRoles(projectName, userDN, issuerDN)).andReturn(null);
        expect(authorizationInterface2.getRoles(projectName, userDN, issuerDN)).andReturn(null);
        expect(authorizationInterface3.getRoles(projectName, userDN, issuerDN)).andReturn(null);
        
        replayAll();
        
        String[] roles = delegatingAuthorizationInterface.getRoles(projectName, userDN, issuerDN);
        assertNull(roles);
        
        verifyAll();
    }
    
    @Test
    public void testGetRolesFirstNonNull() throws Exception {
        String userDN = "testUser";
        String issuerDN = "testIssuer";
        String projectName = "testProject";
        
        expect(authorizationInterface1.getRoles(projectName, userDN, issuerDN)).andReturn(null);
        expect(authorizationInterface2.getRoles(projectName, userDN, issuerDN)).andReturn(new String[] {"test"});
        
        replayAll();
        
        String[] roles = delegatingAuthorizationInterface.getRoles(projectName, userDN, issuerDN);
        assertArrayEquals(new String[] {"test"}, roles);
        
        verifyAll();
    }
}
