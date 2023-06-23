package datawave.security.util;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.junit.Test;

import com.google.common.collect.Lists;

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
