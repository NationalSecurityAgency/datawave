package datawave.security.util;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.*;

public class ProxiedEntityUtilsTest {
    
    @Test
    public void testNormalizedDN() {
        String expected = "cn=john q. doe, ou=my dept, o=my org, c=us";
        String actual = ProxiedEntityUtils.normalizeDN("C=US, O=My Org, OU=My Dept, CN=John Q. Doe");
        assertEquals(expected, actual);
    }
    
    @Test
    public void testSplitProxiedSubjectIssuerDNsForSingleAuth() {
        String auths = "singleAuth";
        String[] authArray = ProxiedEntityUtils.splitProxiedSubjectIssuerDNs(auths);
        assertEquals(1, authArray.length);
        assertEquals(auths, authArray[0]);
    }
    
    @Test
    public void testSplitProxiedSubjectIssuerDNsForProxiedAuths() {
        String dn = "sdn1";
        String issuerDN = "issuerDN";
        String combinedDN = dn + "<" + issuerDN + ">";
        
        String dn2 = "sdn2";
        String combinedDN2 = dn + "<" + issuerDN + ">";
        
        String auths = combinedDN + combinedDN2;
        String[] authArray = ProxiedEntityUtils.splitProxiedSubjectIssuerDNs(auths);
        assertEquals(2, authArray.length);
        assertEquals(dn, authArray[0]);
        assertEquals(issuerDN, authArray[1]);
    }
    
    @Test
    public void testSplitProxiedSubjectIssuerDNsNonProxied() {
        String dn = "sdn1";
        String issuerDN = "issuerDN";
        String combinedDN = dn + "<" + issuerDN + ">";
        
        String[] authArray = ProxiedEntityUtils.splitProxiedSubjectIssuerDNs(combinedDN);
        assertEquals(2, authArray.length);
        assertEquals(dn, authArray[0]);
        assertEquals(issuerDN, authArray[1]);
    }
    
}
