package datawave.security.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ConfigUtils}.
 */
class ConfigUtilsTest {

    @Test
    void testGetStringGivenNullValue() {
        assertEquals("defaultValue", ConfigUtils.getString(null, "defaultValue"));
    }

    @Test
    void testGetStringGivenBlankValue() {
        assertEquals("defaultValue", ConfigUtils.getString("  ", "defaultValue"));
    }

    @Test
    void testGetStringGivenNonBlankValue() {
        assertEquals("value", ConfigUtils.getString(" value ", "defaultValue"));
    }

    @Test
    void testGetBooleanGivenNullValue() {
        assertTrue(ConfigUtils.getBoolean(null, true));
    }

    @Test
    void testGetBooleanGivenBlankValue() {
        assertTrue(ConfigUtils.getBoolean("  ", true));
    }

    @Test
    void testGetBooleanGivenNonBlankValue() {
        assertFalse(ConfigUtils.getBoolean("  false  ", true));
    }

    @Test
    void testGetLongGivenNullValue() {
        assertEquals(10L, ConfigUtils.getLong(null, 10L));
    }

    @Test
    void testGetLongGivenBlankValue() {
        assertEquals(10L, ConfigUtils.getLong("   ", 10L));
    }

    @Test
    void testGetLongGivenNonBlankValue() {
        assertEquals(1000L, ConfigUtils.getLong(" 1000 ", 10L));
    }
}
