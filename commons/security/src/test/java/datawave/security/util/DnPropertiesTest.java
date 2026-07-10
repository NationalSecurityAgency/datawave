package datawave.security.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;

import com.google.common.collect.Sets;

/**
 * Tests for {@link DnProperties}.
 */
class DnPropertiesTest {

    /**
     * Verify that {@link DnProperties#getDefaultInstance()} will load properties from the properties file {@value DnProperties#DEFAULT_PROPERTIES_FILE}.
     */
    @ClearSystemProperty(key = DnProperties.SUBJECT_DN_PATTERN_PROPERTY)
    @ClearSystemProperty(key = DnProperties.NPE_OU_PROPERTY)
    @Test
    void testGetDefaultInstance() {
        Pattern expectedSubjectDnPattern = Pattern.compile("(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)", Pattern.CASE_INSENSITIVE);
        Set<String> expectedNpeOUs = Sets.newHashSet("IAMNOTAPERSON", "NPE", "STILLNOTAPERSON");

        DnProperties actual = DnProperties.getDefaultInstance();
        assertTrue(arePatternsEqual(expectedSubjectDnPattern, actual.getSubjectDnPattern()));
        assertEquals(expectedNpeOUs, actual.getNpeOUs());
    }

    /**
     * Verify that {@link DnProperties#createInstanceFromProperties(String)} is able to load properties from a properties file when no system properties are
     * configured.
     */
    @ClearSystemProperty(key = DnProperties.SUBJECT_DN_PATTERN_PROPERTY)
    @ClearSystemProperty(key = DnProperties.NPE_OU_PROPERTY)
    @Test
    void testCreatingInstanceFromPropertiesFile() {
        Pattern expectedSubjectDnPattern = Pattern.compile("(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)", Pattern.CASE_INSENSITIVE);
        Set<String> expectedNpeOUs = Sets.newHashSet("IAMNOTAPERSON", "NPE", "STILLNOTAPERSON");

        DnProperties actual = DnProperties.createInstanceFromProperties("dnutils.properties");
        assertTrue(arePatternsEqual(expectedSubjectDnPattern, actual.getSubjectDnPattern()));
        assertEquals(expectedNpeOUs, actual.getNpeOUs());
    }

    /**
     * Verify that {@link DnProperties#createInstanceFromProperties(String)} is able to load properties from system properties when no properties file exists.
     */
    @SetSystemProperty(key = DnProperties.SUBJECT_DN_PATTERN_PROPERTY, value = "(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)")
    @SetSystemProperty(key = DnProperties.NPE_OU_PROPERTY, value = "iamnotaperson,npe,stillnotaperson")
    @Test
    void testCreatingInstanceFromSystemProperties() {
        Pattern expectedSubjectDnPattern = Pattern.compile("(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)", Pattern.CASE_INSENSITIVE);
        Set<String> expectedNpeOUs = Sets.newHashSet("IAMNOTAPERSON", "NPE", "STILLNOTAPERSON");

        DnProperties actual = DnProperties.createInstanceFromProperties("nonexistent.properties");
        assertTrue(arePatternsEqual(expectedSubjectDnPattern, actual.getSubjectDnPattern()));
        assertEquals(expectedNpeOUs, actual.getNpeOUs());
    }

    /**
     * Verify that {@link DnProperties#createInstanceFromProperties(String)} will prioritize system properties over the properties file.
     */
    @SetSystemProperty(key = DnProperties.SUBJECT_DN_PATTERN_PROPERTY, value = "(?:^|,)\\s*OU\\s*=\\s*My Other Department\\s*(?:,|$)")
    @SetSystemProperty(key = DnProperties.NPE_OU_PROPERTY, value = "iamnotaperson")
    @Test
    void testSystemPropertiesOverridePropertiesFile() {
        Pattern expectedSubjectDnPattern = Pattern.compile("(?:^|,)\\s*OU\\s*=\\s*My Other Department\\s*(?:,|$)", Pattern.CASE_INSENSITIVE);
        Set<String> expectedNpeOUs = Sets.newHashSet("IAMNOTAPERSON");

        DnProperties actual = DnProperties.createInstanceFromProperties("dnutils.properties");
        assertTrue(arePatternsEqual(expectedSubjectDnPattern, actual.getSubjectDnPattern()));
        assertEquals(expectedNpeOUs, actual.getNpeOUs());
    }

    /**
     * Verify an exception is thrown if the properties file cannot be loaded and either a subject DN pattern or NPE OU list were not specified via system
     * properties.
     */
    @SetSystemProperty(key = DnProperties.SUBJECT_DN_PATTERN_PROPERTY, value = "  ")
    @SetSystemProperty(key = DnProperties.NPE_OU_PROPERTY, value = "  ")
    @Test
    void testNonexistentPropertiesFileThrowsException() {
        Throwable throwable = assertThrows(RuntimeException.class, () -> DnProperties.createInstanceFromProperties("nonexistent.properties"));
        assertEquals("Failed to load properties file nonexistent.properties", throwable.getMessage());
    }

    /**
     * Verify an exception is not thrown for a non-existent properties file if a subject DN pattern and NPE OU list were provided via system properties.
     */
    @SetSystemProperty(key = DnProperties.SUBJECT_DN_PATTERN_PROPERTY, value = "(?:^|,)\\s*OU\\s*=\\s*My Other Department\\s*(?:,|$)")
    @SetSystemProperty(key = DnProperties.NPE_OU_PROPERTY, value = "iamnotaperson")
    @Test
    void testExceptionNotThrownForNonExistentPropertiesFileWhenSystemPropertiesSet() {
        Pattern expectedSubjectDnPattern = Pattern.compile("(?:^|,)\\s*OU\\s*=\\s*My Other Department\\s*(?:,|$)", Pattern.CASE_INSENSITIVE);
        Set<String> expectedNpeOUs = Sets.newHashSet("IAMNOTAPERSON");

        DnProperties actual = DnProperties.createInstanceFromProperties("dnutils.properties");
        assertTrue(arePatternsEqual(expectedSubjectDnPattern, actual.getSubjectDnPattern()));
        assertEquals(expectedNpeOUs, actual.getNpeOUs());
    }

    /**
     * Verify an exception is thrown by {@link DnProperties#createInstanceFromProperties(String)} when no valid subject DN pattern can be loaded.
     */
    @SetSystemProperty(key = DnProperties.SUBJECT_DN_PATTERN_PROPERTY, value = "  ")
    @SetSystemProperty(key = DnProperties.NPE_OU_PROPERTY, value = "iamnotaperson,npe,stillnotaperson")
    @Test
    void testNoValidSubjectDnPattern() {
        Throwable thrown = assertThrows(IllegalArgumentException.class, () -> DnProperties.createInstanceFromProperties("dnutils_blank.properties"));
        assertEquals("Failed to load valid subject DN pattern from property subject.dn.pattern from system or properties file dnutils_blank.properties",
                        thrown.getMessage());
    }

    /**
     * Verify an exception is thrown by {@link DnProperties#createInstanceFromProperties(String)} when no valid NPE OUs can be loaded.
     */
    @SetSystemProperty(key = DnProperties.SUBJECT_DN_PATTERN_PROPERTY, value = "(?:^|,)\\s*OU\\s*=\\s*My Department\\s*(?:,|$)")
    @SetSystemProperty(key = DnProperties.NPE_OU_PROPERTY, value = "  ")
    @Test
    void testNoValidNpeOUs() {
        Throwable thrown = assertThrows(IllegalArgumentException.class, () -> DnProperties.createInstanceFromProperties("dnutils_blank.properties"));
        assertEquals("Failed to load valid NPE OU list from property npe.ou.entries from system or properties file dnutils_blank.properties",
                        thrown.getMessage());
    }

    /**
     * Verify an exception is thrown by {@link DnProperties#createInstanceFromProperties(String)} when the subject DN pattern cannot be compiled.
     */
    @SetSystemProperty(key = DnProperties.SUBJECT_DN_PATTERN_PROPERTY, value = "([sda")
    @SetSystemProperty(key = DnProperties.NPE_OU_PROPERTY, value = "iamnotaperson,npe,stillnotaperson")
    @Test
    void givenUncompilableSubjectDnPattern() {
        Throwable thrown = assertThrows(RuntimeException.class, () -> DnProperties.createInstanceFromProperties("nonexistent.properties"));
        assertEquals("Unable to compile subject DN pattern '([sda'", thrown.getMessage());
        assertInstanceOf(PatternSyntaxException.class, thrown.getCause());
    }

    /**
     * Return whether the given patterns have the same pattern and flags.
     *
     * @param pattern1
     *            the first pattern
     * @param pattern2
     *            the second pattern
     * @return true if the patterns are equal, or false otherwise
     */
    private boolean arePatternsEqual(Pattern pattern1, Pattern pattern2) {
        if (pattern1 == pattern2) {
            return true;
        }
        return pattern1.pattern().equals(pattern2.pattern()) && pattern1.flags() == pattern2.flags();
    }
}
