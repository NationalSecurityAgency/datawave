package datawave.security.util;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DnUtilsTest {
    
    @Test
    public void testBuildNormalizedProxyDN() {
        String expected = "sdn<idn>";
        String actual = DnUtils.buildNormalizedProxyDN("SDN", "IDN", null, null);
        assertEquals(expected, actual);
        
        expected = "sdn2<idn2><sdn1><idn1>";
        actual = DnUtils.buildNormalizedProxyDN("SDN1", "IDN1", "SDN2", "IDN2");
        assertEquals(expected, actual);
        
        expected = "sdn2<idn2><sdn3><idn3><sdn1><idn1>";
        actual = DnUtils.buildNormalizedProxyDN("SDN1", "IDN1", "SDN2<SDN3>", "IDN2<IDN3>");
        assertEquals(expected, actual);
        
        expected = "sdn2<idn2><sdn3><idn3><sdn1><idn1>";
        actual = DnUtils.buildNormalizedProxyDN("SDN1", "IDN1", "<SDN2><SDN3>", "<IDN2><IDN3>");
        assertEquals(expected, actual);
    }
    
    @Test
    public void testBuildNormalizedDN() {
        Collection<String> expected = Lists.newArrayList("sdn", "idn");
        Collection<String> actual = DnUtils.buildNormalizedDNList("SDN", "IDN", null, null);
        assertEquals(expected, actual);
        
        expected = Lists.newArrayList("sdn2", "idn2", "sdn1", "idn1");
        actual = DnUtils.buildNormalizedDNList("SDN1", "IDN1", "SDN2", "IDN2");
        assertEquals(expected, actual);
        
        expected = Lists.newArrayList("sdn2", "idn2", "sdn3", "idn3", "sdn1", "idn1");
        actual = DnUtils.buildNormalizedDNList("SDN1", "IDN1", "SDN2<SDN3>", "IDN2<IDN3>");
        assertEquals(expected, actual);
        
        expected = Lists.newArrayList("sdn2", "idn2", "sdn3", "idn3", "sdn1", "idn1");
        actual = DnUtils.buildNormalizedDNList("SDN1", "IDN1", "<SDN2><SDN3>", "<IDN2><IDN3>");
        assertEquals(expected, actual);
    }
    
    @Test
    public void testGetUserDnFromArray() {
        String userDnForTest = "snd1";
        String[] array = new String[] {userDnForTest, "idn"};
        String userDN = DnUtils.getUserDN(array);
        assertEquals(userDnForTest, userDN);
    }
    
    @Test
    public void testTest() {
        String[] dns = new String[] {"sdn"};
        assertThrows(IllegalArgumentException.class, () -> DnUtils.getUserDN(dns, true));
    }
    
    @Test
    public void testBuildNormalizedProxyDNTooMissingIssuers() {
        assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2<SDN3>", null));
    }
    
    @Test
    public void testBuildNormalizedProxyDNTooFewIssuers() {
        assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2<SDN3>", "IDN2"));
    }
    
    @Test
    public void testBuildNormalizedProxyDNTooFewSubjects() {
        assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2", "IDN2<IDN3>"));
    }
    
    @Test
    public void testBuildNormalizedProxyDNSubjectEqualsIssuer() {
        assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2", "SDN2"));
    }
    
    @Test
    public void testBuildNormalizedProxyDNSubjectDNInIssuer() {
        assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2", "CN=foo,OU=My Department"));
    }
    
    @Test
    public void testBuildNormalizedDNListTooMissingIssuers() {
        assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2<SDN3>", null));
    }
    
    @Test
    public void testBuildNormalizedDNListTooFewIssuers() {
        assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2<SDN3>", "IDN2"));
    }
    
    @Test
    public void testBuildNormalizedDNListTooFewSubjects() {
        assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2", "IDN2<IDN3>"));
    }
    
    @Test
    public void testBuildNormalizedDNListSubjectEqualsIssuer() {
        assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2", "SDN2"));
    }
    
    @Test
    public void testBuildNormalizedDNListSubjectDNInIssuer() {
        assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2", "CN=foo,OU=My Department"));
    }
    
}
