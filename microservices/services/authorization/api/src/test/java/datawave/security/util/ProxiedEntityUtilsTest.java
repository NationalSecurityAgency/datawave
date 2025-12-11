package datawave.security.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ProxiedEntityUtils}.
 */
public class ProxiedEntityUtilsTest {

    /**
     * Tests for {@link ProxiedEntityUtils#splitProxiedDNs(String, boolean)}.
     */
    @Test
    void testSplitProxiedDNs() {
        // Verify that a single DN results in an array with the DN.
        assertArrayEquals(new String[] {"cn=john q. doe, c=us, o=my org, ou=my dept"},
                        ProxiedEntityUtils.splitProxiedDNs("cn=john q. doe, c=us, o=my org, ou=my dept", true));

        // Verify that a single DN with escaped < and > characters results in an array with the DN.
        assertArrayEquals(new String[] {"cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>"},
                        ProxiedEntityUtils.splitProxiedDNs("cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>", true));

        // Verify that multiple DNs result in an array with the DNs.
        assertArrayEquals(new String[] {"cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>", "cn=server1, c=us, o=my org, ou=my dept"}, ProxiedEntityUtils
                        .splitProxiedDNs("cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\><cn=server1, c=us, o=my org, ou=my dept>", true));

        // Verify that duplicate DNs are retained when specified to allow duplicates.
        // @formatter:off
        assertArrayEquals(new String[] {"cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>",
                                        "cn=server1, c=us, o=my org, ou=my dept",
                                        "cn=server1, c=us, o=my org, ou=my dept"},
                        ProxiedEntityUtils.splitProxiedDNs("cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\><cn=server1, c=us, o=my org, ou=my dept><cn=server1, c=us, o=my org, ou=my dept>", true));
        // @formatter:on

        // Verify that only the first instance of a duplicate DN is retained when duplicates are not allowed.
        // Verify that duplicate DNs are retained when specified to allow duplicates.
        // @formatter:off
        assertArrayEquals(new String[] {"cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>",
                                        "cn=server1, c=us, o=my org, ou=my dept",
                                        "cn=server2, c=us, o=my org, ou=my dept"},
                        ProxiedEntityUtils.splitProxiedDNs("cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\><cn=server1, c=us, o=my org, ou=my dept><cn=server2, c=us, o=my org, ou=my dept><cn=server1, c=us, o=my org, ou=my dept>", false));
        // @formatter:on

        // Verify that any blank DNs are pruned.
        assertArrayEquals(new String[] {"cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>", "cn=server1, c=us, o=my org, ou=my dept"}, ProxiedEntityUtils
                        .splitProxiedDNs("cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\><    ><cn=server1, c=us, o=my org, ou=my dept>", true));

    }

    /**
     * Tests for {@link ProxiedEntityUtils#splitProxiedSubjectIssuerDNs(String)}.
     */
    @Test
    void testSplitProxiedSubjectIssuerDNs() {
        // Verify that a single DN results in an array with the DN.
        assertArrayEquals(new String[] {"cn=john q. doe, c=us, o=my org, ou=my dept"},
                        ProxiedEntityUtils.splitProxiedSubjectIssuerDNs("cn=john q. doe, c=us, o=my org, ou=my dept"));

        // Verify that a single DN with escaped < and > characters results in an array with the DN.
        assertArrayEquals(new String[] {"cn=john q. doe, c=us, o=my org, ou=\\<my dept\\>"},
                        ProxiedEntityUtils.splitProxiedSubjectIssuerDNs("cn=john q. doe, c=us, o=my org, ou=\\<my dept\\>"));

        // Verify that an uneven number of DNs greater than one results in an exception.
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                        () -> ProxiedEntityUtils.splitProxiedSubjectIssuerDNs("cn=subject1<cn=issuer1><cn=subject2>"));
        assertEquals("Invalid proxied DNs list does not have entries in pairs.", exception.getMessage());

        // Verify that only the first subject-issuer pair for any unique subject DN is retained.
        assertArrayEquals(new String[] {"cn=subject1", "cn=issuer1"},
                        ProxiedEntityUtils.splitProxiedSubjectIssuerDNs("cn=subject1<cn=issuer1><cn=subject1><cn=issuer2>"));

        // Verify that multiple subject-issuer pairs are parsed correctly.
        assertArrayEquals(new String[] {"cn=subject1", "cn=issuer1", "cn=subject2", "cn=issuer2"},
                        ProxiedEntityUtils.splitProxiedSubjectIssuerDNs("cn=subject1<cn=issuer1><cn=subject2><cn=issuer2>"));

        // Verify that any blank DNs are pruned.
        assertArrayEquals(new String[] {"cn=subject1", "cn=issuer1", "cn=subject2", "cn=issuer2"},
                        ProxiedEntityUtils.splitProxiedSubjectIssuerDNs("cn=subject1<cn=issuer1><    ><cn=subject2><    ><cn=issuer2>"));

    }

    /**
     * Tests for {@link ProxiedEntityUtils#buildProxiedDN(String...)}.
     */
    @Test
    void testBuildProxiedDN() {
        // Verify that a blank string results in the original string.
        assertEquals("   ", ProxiedEntityUtils.buildProxiedDN("   "));

        // Verify that a single DN with no arrows results in the original DN.
        assertEquals("cn=john q. doe, c=us, o=my org, ou=my dept", ProxiedEntityUtils.buildProxiedDN("cn=john q. doe, c=us, o=my org, ou=my dept"));

        // Verify that a single DN with arrows results in the original DN with the arrows escaped.
        assertEquals("cn=john q. doe, c=\\<us\\>, o=my org, ou=\\<my dept\\>",
                        ProxiedEntityUtils.buildProxiedDN("cn=john q. doe, c=<us>, o=my org, ou=<my dept>"));

        // Verify that multiple DNs, some with arrows, result in the DNs concatenated, wrapped by arrows, with original arrows escaped.
        // @formatter:off
        assertEquals("cn=john q. doe, c=us, o=my org, ou=my dept<cn=server2, c=\\<us\\>, o=my org, ou=\\<my dept\\>><cn=server1, c=us, o=my org, ou=my dept>",
                        ProxiedEntityUtils.buildProxiedDN("cn=john q. doe, c=us, o=my org, ou=my dept",
                                        "cn=server2, c=\\<us\\>, o=my org, ou=\\<my dept\\>",
                                        "cn=server1, c=us, o=my org, ou=my dept"));
        // @formatter:on
    }

    /**
     * Tests for {@link ProxiedEntityUtils#getCommonName(String)}.
     */
    @Test
    void testGetCommonName() {
        // Verify that a blank string results in a null CN.
        assertNull(ProxiedEntityUtils.getCommonName("  "));

        // Verify that a non-DN results in a null CN.
        assertNull(ProxiedEntityUtils.getCommonName("S-1-1-0"));

        // Verify that a DN with no CN results in a null CN.
        assertNull(ProxiedEntityUtils.getCommonName("c=us, o=my org, ou=my dept"));

        // Verify that a DN with a single CN returns the value of the CN.
        assertEquals("john q. doe", ProxiedEntityUtils.getCommonName("cn=john q. doe, c=us, o=my org, ou=my dept"));

        // Verify that a DN with multiple CNs returns the value of the last CN.
        assertEquals("john q. doe", ProxiedEntityUtils.getCommonName("cn=johnny q. doe, cn=johnathan q. doe, cn=john q. doe, c=us, o=my org, ou=my dept"));
    }

    /**
     * Tests for {@link ProxiedEntityUtils#getOrganizationalUnits(String)}.
     */
    @Test
    void testGetOrganizationalUnits() {
        // Verify that a blank DN results in an empty array.
        assertEquals(0, ProxiedEntityUtils.getOrganizationalUnits("  ").length);

        // Verify that a DN with no matching OU results in an empty array.
        assertEquals(0, ProxiedEntityUtils.getOrganizationalUnits("cn=john q. doe, c=us, o=my org").length);

        // Verify that a DN with a single OU results in an array with a single element.
        assertArrayEquals(new String[] {"my dept"}, ProxiedEntityUtils.getOrganizationalUnits("cn=john q. doe, c=us, o=my org, ou=my dept"));

        // Verify that a DN with a multiple OUs results in an array with multiple elements.
        assertArrayEquals(new String[] {"my subsidiary", "my dept"},
                        ProxiedEntityUtils.getOrganizationalUnits("cn=john q. doe, c=us, o=my org, ou=my dept, ou=my subsidiary"));

        // Verify that DN that cannot be parsed results in an empty array.
        assertEquals(0, ProxiedEntityUtils.getOrganizationalUnits("S-1-1-0").length);
    }

    /**
     * Tests for {@link ProxiedEntityUtils#getShortName(String)}.
     */
    @Test
    public void testGetShortName() {
        // Verify that a blank string results in empty string.
        assertEquals("", ProxiedEntityUtils.getShortName("  "));

        // Verify that a non-DN results in the last text portion returned.
        assertEquals("apples", ProxiedEntityUtils.getShortName("pears to apples"));

        // Verify that a DN with no CN results in the last text portion returned.
        assertEquals("dept", ProxiedEntityUtils.getShortName("c=us, o=my org, ou=my dept"));

        // Verify that a DN with a single CN results in the last text portion of the CN.
        assertEquals("doe", ProxiedEntityUtils.getShortName("cn=john q. doe, c=us, o=my org, ou=my dept"));

        // Verify that a DN with multiple CNs results in the last text portion of the last CN.
        assertEquals("hart", ProxiedEntityUtils.getShortName("cn=johnny q. doe, cn=jonathan q. buck, cn=john q. hart, c=us, o=my org, ou=my dept"));
    }

    /**
     * Tests for {@link ProxiedEntityUtils#getComponents(String, String)}.
     */
    @Test
    public void testGetComponents() {
        // Verify that a blank DN results in an empty array.
        assertEquals(0, ProxiedEntityUtils.getComponents("  ", "cn").length);

        // Verify that a blank component name results in an empty array.
        assertEquals(0, ProxiedEntityUtils.getComponents("cn=john q. doe, c=us, o=my org, ou=my dept", "  ").length);

        // Verify that a DN with no matching component results in an empty array.
        assertEquals(0, ProxiedEntityUtils.getComponents("cn=john q. doe, c=us, o=my org, ou=my dept", "dc").length);

        // Verify that a DN with a single-value matching component results in an array with a single element.
        assertArrayEquals(new String[] {"my org"}, ProxiedEntityUtils.getComponents("cn=john q. doe, c=us, o=my org, ou=my dept", "o"));

        // Verify that a DN with a multi-value matching component results in an array with multiple elements.
        assertArrayEquals(new String[] {"com", "example"},
                        ProxiedEntityUtils.getComponents("cn=john q. doe, c=us, o=my org, ou=my dept, dc=example, dc=com", "dc"));

        // Verify that component name matching is case-insensitive.
        assertArrayEquals(new String[] {"my org"}, ProxiedEntityUtils.getComponents("cn=john q. doe, c=us, o=my org, ou=my dept", "O"));

        // Verify that DN that cannot be parsed results in an empty array.
        assertEquals(0, ProxiedEntityUtils.getComponents("S-1-1-0", "cn").length);
    }

    /**
     * Tests for {@link ProxiedEntityUtils#normalizeDN(String)}.
     */
    @Test
    public void testNormalizedDN() {
        // Verify the DN is trimmed of whitespace and cast to lowercase.
        assertEquals("c=us, o=my org, cn=john q. doe, ou=my dept", ProxiedEntityUtils.normalizeDN(" C=US, O=My Org, CN=John Q. Doe, OU=My Dept "));

        // Verify that if the last RDN is the CN, that the RDNs are reversed.
        assertEquals("cn=john q. doe, ou=my dept, o=my org, c=us", ProxiedEntityUtils.normalizeDN("C=US, O=My Org, OU=My Dept, CN=John Q. Doe"));

        // Verify that the components are not reordered if the CN is already in the first position.
        assertEquals("cn=john q. doe, c=us, o=my org, ou=my dept", ProxiedEntityUtils.normalizeDN("CN=John Q. Doe, C=US, O=My Org, OU=My Dept"));

        // Verify a string that cannot be parsed as a DN, e.g., a sid, is returned in its original form, trimmed and in lowercase.
        assertEquals("s-1-1-0", ProxiedEntityUtils.normalizeDN(" S-1-1-0 "));
    }
}
