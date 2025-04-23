package datawave.microservice.security.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

public class DnUtilsTest {
    
    private DnUtils dnUtils = new DnUtils(Pattern.compile("(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)", Pattern.CASE_INSENSITIVE),
                    Arrays.asList("iamnotaperson", "npe", "stillnotaperson"));
    
    @Test
    public void testBuildNormalizedProxyDN() {
        String expected = "sdn<idn>";
        String actual = dnUtils.buildNormalizedProxyDN("SDN", "IDN", null, null);
        assertEquals(expected, actual);
        
        expected = "sdn2<idn2><sdn1><idn1>";
        actual = dnUtils.buildNormalizedProxyDN("SDN1", "IDN1", "SDN2", "IDN2");
        assertEquals(expected, actual);
        
        expected = "sdn2<idn2><sdn3><idn3><sdn1><idn1>";
        actual = dnUtils.buildNormalizedProxyDN("SDN1", "IDN1", "SDN2<SDN3>", "IDN2<IDN3>");
        assertEquals(expected, actual);
        
        expected = "sdn2<idn2><sdn3><idn3><sdn1><idn1>";
        actual = dnUtils.buildNormalizedProxyDN("SDN1", "IDN1", "<SDN2><SDN3>", "<IDN2><IDN3>");
        assertEquals(expected, actual);
    }
    
    @Test
    public void testBuildNormalizedDN() {
        Collection<String> expected = Lists.newArrayList("sdn", "idn");
        Collection<String> actual = dnUtils.buildNormalizedDNList("SDN", "IDN", null, null);
        assertEquals(expected, actual);
        
        expected = Lists.newArrayList("sdn2", "idn2", "sdn1", "idn1");
        actual = dnUtils.buildNormalizedDNList("SDN1", "IDN1", "SDN2", "IDN2");
        assertEquals(expected, actual);
        
        expected = Lists.newArrayList("sdn2", "idn2", "sdn3", "idn3", "sdn1", "idn1");
        actual = dnUtils.buildNormalizedDNList("SDN1", "IDN1", "SDN2<SDN3>", "IDN2<IDN3>");
        assertEquals(expected, actual);
        
        expected = Lists.newArrayList("sdn2", "idn2", "sdn3", "idn3", "sdn1", "idn1");
        actual = dnUtils.buildNormalizedDNList("SDN1", "IDN1", "<SDN2><SDN3>", "<IDN2><IDN3>");
        assertEquals(expected, actual);
    }
    
    @Test
    public void testGetUserDnFromArray() {
        String userDnForTest = "snd1";
        String[] array = new String[] {userDnForTest, "idn"};
        String userDN = dnUtils.getUserDN(array);
        assertEquals(userDnForTest, userDN);
    }
    
    @Test
    public void testTest() {
        assertThrows(IllegalArgumentException.class, () -> {
            String[] dns = new String[] {"sdn"};
            dnUtils.getUserDN(dns, true);
        });
    }
    
    @Test
    public void testBuildNormalizedProxyDNTooMissingIssuers() {
        assertThrows(IllegalArgumentException.class, () -> {
            dnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2<SDN3>", null);
        });
    }
    
    @Test
    public void testBuildNormalizedProxyDNTooFewIssuers() {
        assertThrows(IllegalArgumentException.class, () -> {
            dnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2<SDN3>", "IDN2");
        });
    }
    
    @Test
    public void testBuildNormalizedProxyDNTooFewSubjects() {
        assertThrows(IllegalArgumentException.class, () -> {
            dnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2", "IDN2<IDN3>");
        });
    }
    
    @Test
    public void testBuildNormalizedProxyDNSubjectEqualsIssuer() {
        assertThrows(IllegalArgumentException.class, () -> {
            dnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2", "SDN2");
        });
    }
    
    @Test
    public void testBuildNormalizedProxyDNSubjectDNInIssuer() {
        assertThrows(IllegalArgumentException.class, () -> {
            dnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2", "CN=foo,OU=My Department");
        });
    }
    
    @Test
    public void testBuildNormalizedDNListTooMissingIssuers() {
        assertThrows(IllegalArgumentException.class, () -> {
            dnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2<SDN3>", null);
        });
    }
    
    @Test
    public void testBuildNormalizedDNListTooFewIssuers() {
        assertThrows(IllegalArgumentException.class, () -> {
            dnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2<SDN3>", "IDN2");
        });
    }
    
    @Test
    public void testBuildNormalizedDNListTooFewSubjects() {
        assertThrows(IllegalArgumentException.class, () -> {
            dnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2", "IDN2<IDN3>");
        });
    }
    
    @Test
    public void testBuildNormalizedDNListSubjectEqualsIssuer() {
        assertThrows(IllegalArgumentException.class, () -> {
            dnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2", "SDN2");
        });
    }
    
    @Test
    public void testBuildNormalizedDNListSubjectDNInIssuer() {
        assertThrows(IllegalArgumentException.class, () -> {
            dnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2", "CN=foo,OU=My Department");
        });
    }
    
}
