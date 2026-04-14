package datawave.security.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collection;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;

import com.google.common.collect.Lists;

@SetSystemProperty(key = DnUtils.SUBJECT_DN_PATTERN_PROPERTY, value = "(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)")
@SetSystemProperty(key = DnUtils.NPE_OU_PROPERTY, value = "iamnotaperson,npe,stillnotaperson")
class DnUtilsTest {

    /**
     * Tests for {@link DnUtils#splitProxiedDNs(String, boolean)}.
     */
    @Test
    void testSplitProxiedDNs() {
        // Verify that a single DN results in an array with the DN.
        assertArrayEquals(new String[] {"cn=john q. doe, c=us, o=my org, ou=my dept"},
                        DnUtils.splitProxiedDNs("cn=john q. doe, c=us, o=my org, ou=my dept", true));

        // Verify that a single DN with escaped < and > characters results in an array with the DN.
        assertArrayEquals(new String[] {"cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>"},
                        DnUtils.splitProxiedDNs("cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>", true));

        // Verify that multiple DNs result in an array with the DNs.
        assertArrayEquals(new String[] {"cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>", "cn=server1, c=us, o=my org, ou=my dept"},
                        DnUtils.splitProxiedDNs("cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\><cn=server1, c=us, o=my org, ou=my dept>", true));

        // Verify that duplicate DNs are retained when specified to allow duplicates.
        // @formatter:off
        assertArrayEquals(new String[] {"cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>",
                                        "cn=server1, c=us, o=my org, ou=my dept",
                                        "cn=server1, c=us, o=my org, ou=my dept"},
                        DnUtils.splitProxiedDNs("cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\><cn=server1, c=us, o=my org, ou=my dept><cn=server1, c=us, o=my org, ou=my dept>", true));
        // @formatter:on

        // Verify that only the first instance of a duplicate DN is retained when duplicates are not allowed.
        // Verify that duplicate DNs are retained when specified to allow duplicates.
        // @formatter:off
        assertArrayEquals(new String[] {"cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>",
                                        "cn=server1, c=us, o=my org, ou=my dept",
                                        "cn=server2, c=us, o=my org, ou=my dept"},
                        DnUtils.splitProxiedDNs("cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\><cn=server1, c=us, o=my org, ou=my dept><cn=server2, c=us, o=my org, ou=my dept><cn=server1, c=us, o=my org, ou=my dept>", false));
        // @formatter:on

        // Verify that any blank DNs are pruned.
        assertArrayEquals(new String[] {"cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>", "cn=server1, c=us, o=my org, ou=my dept"},
                        DnUtils.splitProxiedDNs("cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\><    ><cn=server1, c=us, o=my org, ou=my dept>", true));

    }

    /**
     * Tests for {@link DnUtils#splitProxiedSubjectIssuerDNs(String)}.
     */
    @Test
    void testSplitProxiedSubjectIssuerDNs() {
        // Verify that a single DN results in an array with the DN.
        assertArrayEquals(new String[] {"cn=john q. doe, c=us, o=my org, ou=my dept"},
                        DnUtils.splitProxiedSubjectIssuerDNs("cn=john q. doe, c=us, o=my org, ou=my dept"));

        // Verify that a single DN with escaped < and > characters results in an array with the DN.
        assertArrayEquals(new String[] {"cn=john q. doe, c=us, o=my org, ou=\\<my dept\\>"},
                        DnUtils.splitProxiedSubjectIssuerDNs("cn=john q. doe, c=us, o=my org, ou=\\<my dept\\>"));

        // Verify that an uneven number of DNs greater than one results in an exception.
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                        () -> DnUtils.splitProxiedSubjectIssuerDNs("cn=subject1<cn=issuer1><cn=subject2>"));
        assertEquals("Invalid proxied DNs list does not have entries in pairs.", exception.getMessage());

        // Verify that only the first subject-issuer pair for any unique subject DN is retained.
        assertArrayEquals(new String[] {"cn=subject1", "cn=issuer1"}, DnUtils.splitProxiedSubjectIssuerDNs("cn=subject1<cn=issuer1><cn=subject1><cn=issuer2>"));

        // Verify that multiple subject-issuer pairs are parsed correctly.
        assertArrayEquals(new String[] {"cn=subject1", "cn=issuer1", "cn=subject2", "cn=issuer2"},
                        DnUtils.splitProxiedSubjectIssuerDNs("cn=subject1<cn=issuer1><cn=subject2><cn=issuer2>"));

        // Verify that any blank DNs are pruned.
        assertArrayEquals(new String[] {"cn=subject1", "cn=issuer1", "cn=subject2", "cn=issuer2"},
                        DnUtils.splitProxiedSubjectIssuerDNs("cn=subject1<cn=issuer1><    ><cn=subject2><    ><cn=issuer2>"));

    }

    /**
     * Tests for {@link DnUtils#buildProxiedDN(String...)}.
     */
    @Test
    void testBuildProxiedDN() {
        // Verify that a blank string results in the original string.
        assertEquals("   ", DnUtils.buildProxiedDN("   "));

        // Verify that a single DN with no arrows results in the original DN.
        assertEquals("cn=john q. doe, c=us, o=my org, ou=my dept", DnUtils.buildProxiedDN("cn=john q. doe, c=us, o=my org, ou=my dept"));

        // Verify that a single DN with arrows results in the original DN with the arrows escaped.
        assertEquals("cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>", DnUtils.buildProxiedDN("cn=john q. doe, c=<us>, o=my org, ou=<my dept>"));

        // Verify that multiple DNs, some with arrows, result in the DNs concatenated, wrapped by arrows, with original arrows escaped.
        // @formatter:off
        assertEquals("cn=john q. doe, c=us, o=my org, ou=my dept<cn=server2, c=\\<us\\>, o=my org, ou=\\<my dept\\>><cn=server1, c=us, o=my org, ou=my dept>",
                        DnUtils.buildProxiedDN("cn=john q. doe, c=us, o=my org, ou=my dept",
                                        "cn=server2, c=\\<us\\>, o=my org, ou=\\<my dept\\>",
                                        "cn=server1, c=us, o=my org, ou=my dept"));
        // @formatter:on
    }

    /**
     * Tests for {@link DnUtils#getCommonName(String)}.
     */
    @Test
    void testGetCommonName() {
        // Verify that a blank string results in a null CN.
        assertNull(DnUtils.getCommonName("  "));

        // Verify that a non-DN results in a null CN.
        assertNull(DnUtils.getCommonName("S-1-1-0"));

        // Verify that a DN with no CN results in a null CN.
        assertNull(DnUtils.getCommonName("c=us, o=my org, ou=my dept"));

        // Verify that a DN with a single CN returns the value of the CN.
        assertEquals("john q. doe", DnUtils.getCommonName("cn=john q. doe, c=us, o=my org, ou=my dept"));

        // Verify that a DN with multiple CNs returns the value of the last CN.
        assertEquals("john q. doe", DnUtils.getCommonName("cn=johnny q. doe, cn=johnathan q. doe, cn=john q. doe, c=us, o=my org, ou=my dept"));
    }

    /**
     * Tests for {@link DnUtils#getOrganizationalUnits(String)}.
     */
    @Test
    void testGetOrganizationalUnits() {
        // Verify that a blank DN results in an empty array.
        assertEquals(0, DnUtils.getOrganizationalUnits("  ").length);

        // Verify that a DN with no matching OU results in an empty array.
        assertEquals(0, DnUtils.getOrganizationalUnits("cn=john q. doe, c=us, o=my org").length);

        // Verify that a DN with a single OU results in an array with a single element.
        assertArrayEquals(new String[] {"my dept"}, DnUtils.getOrganizationalUnits("cn=john q. doe, c=us, o=my org, ou=my dept"));

        // Verify that a DN with a multiple OUs results in an array with multiple elements.
        assertArrayEquals(new String[] {"my subsidiary", "my dept"},
                        DnUtils.getOrganizationalUnits("cn=john q. doe, c=us, o=my org, ou=my dept, ou=my subsidiary"));

        // Verify that DN that cannot be parsed results in an empty array.
        assertEquals(0, DnUtils.getOrganizationalUnits("S-1-1-0").length);
    }

    /**
     * Tests for {@link DnUtils#getShortName(String)}.
     */
    @Test
    public void testGetShortName() {
        // Verify that a blank string results in empty string.
        assertEquals("", DnUtils.getShortName("  "));

        // Verify that a non-DN results in the last text portion returned.
        assertEquals("apples", DnUtils.getShortName("pears to apples"));

        // Verify that a DN with no CN results in the last text portion returned.
        assertEquals("dept", DnUtils.getShortName("c=us, o=my org, ou=my dept"));

        // Verify that a DN with a single CN results in the last text portion of the CN.
        assertEquals("doe", DnUtils.getShortName("cn=john q. doe, c=us, o=my org, ou=my dept"));

        // Verify that a DN with multiple CNs results in the last text portion of the last CN.
        assertEquals("hart", DnUtils.getShortName("cn=johnny q. doe, cn=jonathan q. buck, cn=john q. hart, c=us, o=my org, ou=my dept"));
    }

    /**
     * Tests for {@link DnUtils#getComponents(String, String)}.
     */
    @Test
    public void testGetComponents() {
        // Verify that a blank DN results in an empty array.
        assertEquals(0, DnUtils.getComponents("  ", "cn").length);

        // Verify that a blank component name results in an empty array.
        assertEquals(0, DnUtils.getComponents("cn=john q. doe, c=us, o=my org, ou=my dept", "  ").length);

        // Verify that a DN with no matching component results in an empty array.
        assertEquals(0, DnUtils.getComponents("cn=john q. doe, c=us, o=my org, ou=my dept", "dc").length);

        // Verify that a DN with a single-value matching component results in an array with a single element.
        assertArrayEquals(new String[] {"my org"}, DnUtils.getComponents("cn=john q. doe, c=us, o=my org, ou=my dept", "o"));

        // Verify that a DN with a multi-value matching component results in an array with multiple elements.
        assertArrayEquals(new String[] {"com", "example"}, DnUtils.getComponents("cn=john q. doe, c=us, o=my org, ou=my dept, dc=example, dc=com", "dc"));

        // Verify that component name matching is case-insensitive.
        assertArrayEquals(new String[] {"my org"}, DnUtils.getComponents("cn=john q. doe, c=us, o=my org, ou=my dept", "O"));

        // Verify that DN that cannot be parsed results in an empty array.
        assertEquals(0, DnUtils.getComponents("S-1-1-0", "cn").length);
    }

    /**
     * Tests for {@link DnUtils#normalizeDN(String)}.
     */
    @Test
    public void testNormalizedDN() {
        // Verify the DN is trimmed of whitespace and cast to lowercase.
        assertEquals("c=us, o=my org, cn=john q. doe, ou=my dept", DnUtils.normalizeDN(" C=US, O=My Org, CN=John Q. Doe, OU=My Dept "));

        // Verify that if the last RDN is the CN, that the RDNs are reversed.
        assertEquals("cn=john q. doe, ou=my dept, o=my org, c=us", DnUtils.normalizeDN("C=US, O=My Org, OU=My Dept, CN=John Q. Doe"));

        // Verify that the components are not reordered if the CN is already in the first position.
        assertEquals("cn=john q. doe, c=us, o=my org, ou=my dept", DnUtils.normalizeDN("CN=John Q. Doe, C=US, O=My Org, OU=My Dept"));

        // Verify a string that cannot be parsed as a DN, e.g., a sid, is returned in its original form, trimmed and in lowercase.
        assertEquals("s-1-1-0", DnUtils.normalizeDN(" S-1-1-0 "));
    }

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
        assertThrows(IllegalArgumentException.class, () -> {
            String[] dns = new String[] {"sdn"};
            DnUtils.getUserDN(dns, true);
        });
    }

    @Test
    public void testBuildNormalizedProxyDNTooMissingIssuers() {
        assertThrows(IllegalArgumentException.class, () -> {
            DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2<SDN3>", null);
        });
    }

    @Test
    public void testBuildNormalizedProxyDNTooFewIssuers() {
        assertThrows(IllegalArgumentException.class, () -> {
            DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2<SDN3>", "IDN2");
        });
    }

    @Test
    public void testBuildNormalizedProxyDNTooFewSubjects() {
        assertThrows(IllegalArgumentException.class, () -> {
            DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2", "IDN2<IDN3>");
        });
    }

    @Test
    public void testBuildNormalizedProxyDNSubjectEqualsIssuer() {
        assertThrows(IllegalArgumentException.class, () -> {
            DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2", "SDN2");
        });
    }

    @Test
    public void testBuildNormalizedProxyDNSubjectDNInIssuer() {
        assertThrows(IllegalArgumentException.class, () -> {
            DnUtils.buildNormalizedProxyDN("SDN", "IDN", "SDN2", "CN=foo,OU=My Department");
        });
    }

    @Test
    public void testBuildNormalizedDNListTooMissingIssuers() {
        assertThrows(IllegalArgumentException.class, () -> {
            DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2<SDN3>", null);
        });
    }

    @Test
    public void testBuildNormalizedDNListTooFewIssuers() {
        assertThrows(IllegalArgumentException.class, () -> {
            DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2<SDN3>", "IDN2");
        });
    }

    @Test
    public void testBuildNormalizedDNListTooFewSubjects() {
        assertThrows(IllegalArgumentException.class, () -> {
            DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2", "IDN2<IDN3>");
        });
    }

    @Test
    public void testBuildNormalizedDNListSubjectEqualsIssuer() {
        assertThrows(IllegalArgumentException.class, () -> {
            DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2", "SDN2");
        });
    }

    @Test
    public void testBuildNormalizedDNListSubjectDNInIssuer() {
        assertThrows(IllegalArgumentException.class, () -> {
            DnUtils.buildNormalizedDNList("SDN", "IDN", "SDN2", "CN=foo,OU=My Department");
        });
    }
}
