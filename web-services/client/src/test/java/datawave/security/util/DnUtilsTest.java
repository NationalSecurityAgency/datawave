package datawave.security.util;

import static org.junit.Assert.*;

import java.util.Collection;

import org.junit.Test;

import com.google.common.collect.Lists;

public class DnUtilsTest {
    
    @Test
    public void testBuildNormalizedProxyDN() {
        String expected = "sdn<idn>";
        String actual = DnUtils.buildNormalizedProxyDN("SDN", "IDN", null, null);
        assertEquals(expected, actual);
        
        expected = "sdn1<idn1><sdn2><idn2>";
        actual = DnUtils.buildNormalizedProxyDN("SDN1", "IDN1", "SDN2", "IDN2");
        assertEquals(expected, actual);
        
        expected = "sdn1<idn1><sdn2><idn2><sdn3><idn3>";
        actual = DnUtils.buildNormalizedProxyDN("SDN1", "IDN1", "SDN2<SDN3>", "IDN2<IDN3>");
        assertEquals(expected, actual);
        
        expected = "sdn1<idn1><sdn2><idn2><sdn3><idn3>";
        actual = DnUtils.buildNormalizedProxyDN("SDN1", "IDN1", "<SDN2><SDN3>", "<IDN2><IDN3>");
        assertEquals(expected, actual);
    }
    
    @Test
    public void testBuildNormalizedDN() {
        Collection<String> expected = Lists.newArrayList("sdn", "idn");
        Collection<String> actual = DnUtils.buildNormalizedDNList("SDN", "IDN", null, null);
        assertEquals(expected, actual);
        
        expected = Lists.newArrayList("sdn1", "idn1", "sdn2", "idn2");
        actual = DnUtils.buildNormalizedDNList("SDN1", "IDN1", "SDN2", "IDN2");
        assertEquals(expected, actual);
        
        expected = Lists.newArrayList("sdn1", "idn1", "sdn2", "idn2", "sdn3", "idn3");
        actual = DnUtils.buildNormalizedDNList("SDN1", "IDN1", "SDN2<SDN3>", "IDN2<IDN3>");
        assertEquals(expected, actual);
        
        expected = Lists.newArrayList("sdn1", "idn1", "sdn2", "idn2", "sdn3", "idn3");
        actual = DnUtils.buildNormalizedDNList("SDN1", "IDN1", "<SDN2><SDN3>", "<IDN2><IDN3>");
        assertEquals(expected, actual);
    }
    
    @Test
    public void testSplitProxiedSubjectIssuerDNsForSingleAuth() {
        String auths = "singleAuth";
        String[] authArray = DnUtils.splitProxiedSubjectIssuerDNs(auths);
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
        String[] authArray = DnUtils.splitProxiedSubjectIssuerDNs(auths);
        assertEquals(2, authArray.length);
        assertEquals(dn, authArray[0]);
        assertEquals(issuerDN, authArray[1]);
    }
    
    @Test
    public void testSplitProxiedSubjectIssuerDNsNonProxied() {
        String dn = "sdn1";
        String issuerDN = "issuerDN";
        String combinedDN = dn + "<" + issuerDN + ">";
        
        String[] authArray = DnUtils.splitProxiedSubjectIssuerDNs(combinedDN);
        assertEquals(2, authArray.length);
        assertEquals(dn, authArray[0]);
        assertEquals(issuerDN, authArray[1]);
    }
    
    @Test
    public void testGetUserDnFromArray() {
        String userDnForTest = "snd1";
        String[] array = new String[] {userDnForTest, "idn"};
        String userDN = DnUtils.getUserDN(array);
        assertEquals(userDnForTest, userDN);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testTest() {
        String[] dns = new String[] {"sdn"};
        DnUtils.getUserDN(dns, true);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBuildNormalizedProxyDNTooMissingIssuers() {
        DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2<SDN3>", null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBuildNormalizedProxyDNTooFewIssuers() {
        DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2<SDN3>", "IDN2");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBuildNormalizedProxyDNTooFewSubjects() {
        DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2", "IDN2<IDN3>");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBuildNormalizedProxyDNSubjectEqualsIssuer() {
        DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2", "SDN2");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBuildNormalizedProxyDNSubjectDNInIssuer() {
        DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2", "CN=foo,OU=My Department");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBuildNormalizedDNListTooMissingIssuers() {
        DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2<SDN3>", null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBuildNormalizedDNListTooFewIssuers() {
        DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2<SDN3>", "IDN2");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBuildNormalizedDNListTooFewSubjects() {
        DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2", "IDN2<IDN3>");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBuildNormalizedDNListSubjectEqualsIssuer() {
        DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2", "SDN2");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBuildNormalizedDNListSubjectDNInIssuer() {
        DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2", "CN=foo,OU=My Department");
    }
    
}
