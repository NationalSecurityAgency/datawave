package datawave.security.realm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DatawaveEvidenceDecoder.Config}.
 */
class DatawaveEvidenceDecoderConfigTests {

    /**
     * Verify the default values from {@link DatawaveEvidenceDecoder.Config#fromMap(Map)}.
     */
    @Test
    void testDefaultValues() {
        DatawaveEvidenceDecoder.Config config = DatawaveEvidenceDecoder.Config.fromMap(Map.of());
        assertFalse(config.isJwtEnabled());
        assertFalse(config.isTrustedHeadersEnabled());
        assertEquals(-1, config.getMaxCacheEntries());
        assertEquals(-1, config.getMaxCacheAge());
    }

    /**
     * Verify non-default values for {@link DatawaveEvidenceDecoder.Config#fromMap(Map)}.
     */
    @Test
    void testNonDefaultValues() {
        Map<String,String> configMap = new HashMap<>();
        configMap.put(DatawaveEvidenceDecoder.Config.OPTION_JWT_ENABLED, "true");
        configMap.put(DatawaveEvidenceDecoder.Config.OPTION_TRUSTED_HEADERS_ENABLED, "true");
        configMap.put(DatawaveEvidenceDecoder.Config.OPTION_MAX_CACHE_ENTRIES, "100");
        configMap.put(DatawaveEvidenceDecoder.Config.OPTION_MAX_CACHE_AGE, "1000");

        DatawaveEvidenceDecoder.Config config = DatawaveEvidenceDecoder.Config.fromMap(configMap);
        assertTrue(config.isJwtEnabled());
        assertTrue(config.isTrustedHeadersEnabled());
        assertEquals(100L, config.getMaxCacheEntries());
        assertEquals(1000L, config.getMaxCacheAge());
    }
}
