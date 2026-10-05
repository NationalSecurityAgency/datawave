package datawave.security.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

class SubjectIssuerDNPairTest {

    private static final ObjectMapper mapper = JsonMapper.builder().build();

    /**
     * Tests for {@link SubjectIssuerDNPair#of(String)}.
     */
    @Test
    void testOfSubjectDn() {
        // Verify that the subject DN is normalized and that the issuer DN is null.
        SubjectIssuerDNPair pair = SubjectIssuerDNPair.of(" CN=Subject, C=US, O=My Org, OU=My Dept ");
        assertEquals("cn=subject, c=us, o=my org, ou=my dept", pair.subjectDN());
        assertNull(pair.issuerDN());
        assertEquals("cn=subject, c=us, o=my org, ou=my dept<>", pair.toString());

        // Verify an exception occurs for a null subject DN.
        NullPointerException exception = assertThrows(NullPointerException.class, () -> SubjectIssuerDNPair.of(null));
        assertEquals("Parameter subjectDN must not be null", exception.getMessage());
    }

    /**
     * Tests for {@link SubjectIssuerDNPair#of(String, String)}.
     */
    @Test
    void testOfSubjectDNAndIssuerDn() {
        // Verify that the subject DN is normalized and that the issuer DN is null.
        SubjectIssuerDNPair pair = SubjectIssuerDNPair.of(" CN=Subject, C=US, O=My Org, OU=My Dept ", null);
        assertEquals("cn=subject, c=us, o=my org, ou=my dept", pair.subjectDN());
        assertNull(pair.issuerDN());
        assertEquals("cn=subject, c=us, o=my org, ou=my dept<>", pair.toString());

        // Verify that both the subject DN and issuer DN are normalized.
        pair = SubjectIssuerDNPair.of(" CN=Subject, C=US, O=My Org, OU=My Dept ", " CN=Issuer, C=US, O=My Org, OU=My Dept ");
        assertEquals("cn=subject, c=us, o=my org, ou=my dept", pair.subjectDN());
        assertEquals("cn=issuer, c=us, o=my org, ou=my dept", pair.issuerDN());
        assertEquals("cn=subject, c=us, o=my org, ou=my dept<cn=issuer, c=us, o=my org, ou=my dept>", pair.toString());

        // Verify an exception occurs for a null subject DN.
        NullPointerException exception = assertThrows(NullPointerException.class, () -> SubjectIssuerDNPair.of(null, "cn=issuer, c=us, o=my org, ou=my dept"));
        assertEquals("Parameter subjectDN must not be null", exception.getMessage());
    }

    /**
     * Tests for {@link SubjectIssuerDNPair#parse(String)}.
     */
    @Test
    void testParse() {
        // Verify an exception is thrown when parsing a blank string.
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> SubjectIssuerDNPair.parse(" "));
        assertEquals("' ' must contain a single subject and issuer DN", exception.getMessage());

        // Verify an exception is thrown when parsing a string that contains only one DN.
        exception = assertThrows(IllegalArgumentException.class, () -> SubjectIssuerDNPair.parse(" CN=Subject, C=US, O=My Org, OU=My Dept "));
        assertEquals("' CN=Subject, C=US, O=My Org, OU=My Dept ' must contain a single subject and issuer DN", exception.getMessage());

        // Verify an exception is thrown when parsing a string that contains more than two DNs.
        exception = assertThrows(IllegalArgumentException.class, () -> SubjectIssuerDNPair
                        .parse(" CN=Subject, C=US, O=My Org, OU=My Dept <CN=Issuer, C=US, O=My Org, OU=My Dept><CN=OtherSubject, C=US, O=My Org, OU=My Dept>"));
        assertEquals("Invalid proxied DNs list does not have entries in pairs.", exception.getMessage());

        // Verify that both the subject DN and issuer DN are normalized.
        SubjectIssuerDNPair pair = SubjectIssuerDNPair.parse(" CN=Subject, C=US, O=My Org, OU=My Dept< CN=Issuer, C=US, O=My Org, OU=My Dept >");
        assertEquals("cn=subject, c=us, o=my org, ou=my dept", pair.subjectDN());
        assertEquals("cn=issuer, c=us, o=my org, ou=my dept", pair.issuerDN());
        assertEquals("cn=subject, c=us, o=my org, ou=my dept<cn=issuer, c=us, o=my org, ou=my dept>", pair.toString());
    }

    /**
     * Serialization tests
     */
    @Test
    void testSerialization() throws JsonProcessingException {
        SubjectIssuerDNPair pair = SubjectIssuerDNPair.of("cn=subject, c=us, o=my org, ou=my dept", "cn=issuer, c=us, o=my org, ou=my dept");
        String expected = "{\"subjectDN\":\"cn=subject, c=us, o=my org, ou=my dept\",\"issuerDN\":\"cn=issuer, c=us, o=my org, ou=my dept\"}";
        String actual = mapper.writeValueAsString(pair);
        assertEquals(expected, actual);
    }

    /**
     * Deserialization tests.
     */
    @Test
    void testDeserialization() throws JsonProcessingException {
        String json = "{\"subjectDN\":\"cn=subject, c=us, o=my org, ou=my dept\",\"issuerDN\":\"cn=issuer, c=us, o=my org, ou=my dept\"}";
        SubjectIssuerDNPair expected = SubjectIssuerDNPair.of("cn=subject, c=us, o=my org, ou=my dept", "cn=issuer, c=us, o=my org, ou=my dept");
        SubjectIssuerDNPair actual = mapper.readValue(json, SubjectIssuerDNPair.class);
        assertEquals(expected, actual);
    }
}
