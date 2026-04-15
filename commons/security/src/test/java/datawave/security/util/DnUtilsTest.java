package datawave.security.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import datawave.security.authorization.SubjectIssuerDNPair;

class DnUtilsTest {

    private static final Pattern subjectDnPattern = Pattern.compile("(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)", Pattern.CASE_INSENSITIVE);
    private static final List<String> npeOUs = List.of("IAMNOTAPERSON", "NPE", "STILLNOTAPERSON");

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

    /**
     * Tests for {@link DnUtils#buildNormalizedDNList(String, String, String, String, Pattern)}.
     */
    @Test
    public void testBuildNormalizedDNList() {
        // Verify that given no proxied subject or issuer DNs, the list consists of the normalized subject and issuer Dn.
        List<String> expected = Lists.newArrayList("sdn", "idn");
        List<String> actual = DnUtils.buildNormalizedDNList("SDN", "IDN", null, null, subjectDnPattern);
        assertEquals(expected, actual);

        // Verify that given a single proxied subject and issuer dn, the list contains them in the correct order.
        expected = Lists.newArrayList("sdn2", "idn2", "sdn1", "idn1");
        actual = DnUtils.buildNormalizedDNList("SDN1", "IDN1", "SDN2", "IDN2", subjectDnPattern);
        assertEquals(expected, actual);

        // Verify that given multiple proxied subject and issuer DNs, the list contains them in the correct order.
        expected = Lists.newArrayList("sdn2", "idn2", "sdn3", "idn3", "sdn1", "idn1");
        actual = DnUtils.buildNormalizedDNList("SDN1", "IDN1", "SDN2<SDN3>", "IDN2<IDN3>", subjectDnPattern);
        assertEquals(expected, actual);

        // Verify that an exception is thrown if proxied subject DNs are given, but proxied issuer DNs are not.
        Throwable throwable = assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedDNList("SDN1", "IDN1", "SDN2", null, subjectDnPattern));
        assertEquals("If proxied subject DNs are supplied, then issuer DNs must be supplied as well.", throwable.getMessage());

        // Verify that an exception is thrown if an unequal number of subject DNs and issuer DNs were supplied.
        throwable = assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedDNList("SDN1", "IDN2", "SDN2<SDN3>", "IDN2", subjectDnPattern));
        assertEquals("Subject and issuer DN lists do not have the same number of entries: [SDN2, SDN3] vs [IDN2]", throwable.getMessage());

        // Verify that an exception is thrown if a proxied subject DN is equal to its issuer DN.
        throwable = assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedDNList("SDN1", "IDN2", "SDN2", "SDN2", subjectDnPattern));
        assertEquals("Subject DN sdn2 was passed as an issuer DN.", throwable.getMessage());

        // Verify that an exception is thrown if a proxied issuer DN matches the subject DN pattern.
        throwable = assertThrows(IllegalArgumentException.class,
                        () -> DnUtils.buildNormalizedDNList("SDN1", "IDN2", "SDN2", "CN=foo,OU=My Department", subjectDnPattern));
        assertEquals("It appears that a subject DN (cn=foo, ou=my department) was passed as an issuer DN.", throwable.getMessage());
    }

    /**
     * Tests for {@link DnUtils#buildNormalizedProxyDN(String, String, String, String, Pattern)}.
     */
    @Test
    public void testBuildNormalizedProxyDNGivenStringArgs() {
        // Verify that given no proxied subject or issuer DN, string consists of the normalized subject and issuer DN.
        String expected = "sdn<idn>";
        String actual = DnUtils.buildNormalizedProxyDN("SDN", "IDN", null, null, subjectDnPattern);
        assertEquals(expected, actual);

        // Verify that given a single proxied subject and issuer dn, the string contains them in the correct order.
        expected = "sdn2<idn2><sdn1><idn1>";
        actual = DnUtils.buildNormalizedProxyDN("SDN1", "IDN1", "SDN2", "IDN2", subjectDnPattern);
        assertEquals(expected, actual);

        // Verify that given multiple proxied subject and issuer DNs, the string contains them in the correct order.
        expected = "sdn2<idn2><sdn3><idn3><sdn1><idn1>";
        actual = DnUtils.buildNormalizedProxyDN("SDN1", "IDN1", "SDN2<SDN3>", "IDN2<IDN3>", subjectDnPattern);
        assertEquals(expected, actual);

        // Verify that an exception is thrown if proxied subject DNs are given, but proxied issuer DNs are not.
        Throwable throwable = assertThrows(IllegalArgumentException.class,
                        () -> DnUtils.buildNormalizedProxyDN("SDN1", "IDN1", "SDN2", null, subjectDnPattern));
        assertEquals("If proxied subject DNs are supplied, then issuer DNs must be supplied as well.", throwable.getMessage());

        // Verify that an exception is thrown if an unequal number of subject DNs and issuer DNs were supplied.
        throwable = assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedProxyDN("SDN1", "IDN2", "SDN2<SDN3>", "IDN2", subjectDnPattern));
        assertEquals("Subject and issuer DN lists do not have the same number of entries: [SDN2, SDN3] vs [IDN2]", throwable.getMessage());

        // Verify that an exception is thrown if a proxied subject DN is equal to its issuer DN.
        throwable = assertThrows(IllegalArgumentException.class, () -> DnUtils.buildNormalizedProxyDN("SDN1", "IDN2", "SDN2", "SDN2", subjectDnPattern));
        assertEquals("Subject DN sdn2 was passed as an issuer DN.", throwable.getMessage());

        // Verify that an exception is thrown if a proxied issuer DN matches the subject DN pattern.
        throwable = assertThrows(IllegalArgumentException.class,
                        () -> DnUtils.buildNormalizedProxyDN("SDN1", "IDN2", "SDN2", "CN=foo,OU=My Department", subjectDnPattern));
        assertEquals("It appears that a subject DN (cn=foo, ou=my department) was passed as an issuer DN.", throwable.getMessage());
    }

    /**
     * Tests for {@link DnUtils#buildNormalizedProxyDN(List)}.
     */
    @Test
    public void testBuildNormalizedProxyDNGivenSubjectIssuerDNPairArgs() {
        // Verify that given an empty list, a blank string is returned.
        assertEquals("", DnUtils.buildNormalizedProxyDN(List.of()));

        // Verify that given a single SubjectIssuerDnPair, the string consists of the normalized subject and issuer DN.
        assertEquals("sdn<idn>", DnUtils.buildNormalizedProxyDN(List.of(SubjectIssuerDNPair.of("SDN", "IDN"))));

        // Verify that given multiple SubjectIssuerDnPairs, the string consists of them in normalized form.
        assertEquals("sdn2<idn2><sdn3><idn3><sdn1><idn1>", DnUtils.buildNormalizedProxyDN(
                        List.of(SubjectIssuerDNPair.of("SDN2", "IDN2"), SubjectIssuerDNPair.of("SDN3", "IDN3"), SubjectIssuerDNPair.of("SDN1", "IDN1"))));
    }

    /**
     * Tests for {@link DnUtils#isServerDN(String, Collection)}.
     */
    @Test
    void testIsServerDN() {
        // Verify that given a DN with an OU that is in the NPE OUs, true is returned.
        assertTrue(DnUtils.isServerDN("cn=serverA, OU=npe, OU=Other OU", npeOUs));

        // Verify that given a DN without a NPE OU, false is returned.

        // Verify that given a DN with an OU that is in the NPE OUs, true is returned.
        assertFalse(DnUtils.isServerDN("cn=serverA, OU=Example, OU=Other OU", npeOUs));
    }

    /**
     * Tests for {@link DnUtils#getUserDN(String[], Collection)}
     */
    @Test
    void testGetUserDNGivenSubjectDNsOnly() {
        // Verify that given an array that does not contain a user DN, null is returned.
        assertNull(DnUtils.getUserDN(new String[] {"CN=Server1,OU=Npe", "CN=Server2,OU=IAmNotAPerson"}, npeOUs));

        // Verify that given an array with user DNs, the first user DN is returned.
        assertEquals("CN=User1,OU=Example", DnUtils
                        .getUserDN(new String[] {"CN=Server1,OU=Npe", "CN=User1,OU=Example", "CN=Server2,OU=IAmNotAPerson", "CN=User2,OU=Example"}, npeOUs));
    }

    /**
     * Tests for {@link DnUtils#getUserDN(String[], boolean, Collection)}.
     */
    @Test
    void testGetUserDNGivenSubjectAndIssuerDNs() {
        // Verify that given an array that does not contain a user DN, null is returned.
        assertNull(DnUtils.getUserDN(new String[] {"CN=Server1,OU=Npe", "CN=Server2,OU=IAmNotAPerson"}, false, npeOUs));

        // Verify that given an array with user DNs, the first user DN is returned.
        assertEquals("CN=User1,OU=Example", DnUtils.getUserDN(new String[] {"CN=Server1,OU=Npe", "CN=User1,OU=Example", "CN=User2,OU=Example"}, false, npeOUs));

        // Verify that given an array with subject and issuer DNs, that only the subject DNs are examined.
        assertNull(DnUtils.getUserDN(new String[] {"CN=Server1,OU=Npe", "CN=Issuer1,OU=Example", "CN=Server2,OU=IAmNotAPerson", "CN=Issuer2,OU=Example"}, true,
                        npeOUs));
        assertEquals("CN=User,OU=Example", DnUtils
                        .getUserDN(new String[] {"CN=Server1,OU=Npe", "CN=Issuer1,OU=Example", "CN=User,OU=Example", "CN=Issuer2,OU=Example"}, true, npeOUs));

        // Verify that an exception is thrown if containsIssuerDns is true, but the DN array has an uneven length.
        Throwable throwable = assertThrows(IllegalArgumentException.class,
                        () -> DnUtils.getUserDN(new String[] {"CN=Server1,OU=Npe", "CN=Issuer1,OU=Example", "CN=User,OU=Example"}, true, npeOUs));
        assertEquals("DNs array is not a subject/issuer DN list: [CN=Server1,OU=Npe, CN=Issuer1,OU=Example, CN=User,OU=Example]", throwable.getMessage());
    }
}
