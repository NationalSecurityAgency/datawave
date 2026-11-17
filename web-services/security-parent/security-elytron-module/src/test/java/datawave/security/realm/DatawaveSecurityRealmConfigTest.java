package datawave.security.realm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import datawave.security.cert.DatawaveCertVerifier;

/**
 * Tests for {@link DatawaveSecurityRealm.Config}.
 */
public class DatawaveSecurityRealmConfigTest {

    /**
     * Verify the default values returned from {@link DatawaveSecurityRealm.Config#fromMap(Map)}.
     */
    @Test
    void testDefaultValues() {
        DatawaveSecurityRealm.Config config = DatawaveSecurityRealm.Config.fromMap(Map.of());
        assertNull(config.getCertVerifierClass());
        assertNull(config.getOscpLevel());
        assertEquals(-1L, config.getMaxCacheEntries());
        assertEquals(-1L, config.getMaxCacheAge());
        assertNull(config.getRolePropertiesPath());
    }

    /**
     * Verify the configuration returned from {@link DatawaveSecurityRealm.Config#fromMap(Map)} given non-default values.
     */
    @Test
    void testNonDefaultValues() {
        Map<String,String> configMap = new HashMap<>();
        configMap.put(DatawaveSecurityRealm.Config.OPTION_CERT_VERIFIER, "  " + DatawaveCertVerifier.class.getName() + "  ");
        configMap.put(DatawaveSecurityRealm.Config.OPTION_OSCP_LEVEL, "  " + DatawaveCertVerifier.OcspLevel.OFF + "  ");
        configMap.put(DatawaveSecurityRealm.Config.OPTION_MAX_CACHE_ENTRIES, "  100  ");
        configMap.put(DatawaveSecurityRealm.Config.OPTION_MAX_CACHE_AGE, "  1000  ");
        configMap.put(DatawaveSecurityRealm.Config.OPTION_ROLE_PROPERTIES, "  /path/to/roles.properties  ");

        DatawaveSecurityRealm.Config config = DatawaveSecurityRealm.Config.fromMap(configMap);
        assertEquals(DatawaveCertVerifier.class.getName(), config.getCertVerifierClass());
        assertEquals(DatawaveCertVerifier.OcspLevel.OFF.toString(), config.getOscpLevel());
        assertEquals(100L, config.getMaxCacheEntries());
        assertEquals(1000L, config.getMaxCacheAge());
        assertEquals("/path/to/roles.properties", config.getRolePropertiesPath());
    }
}
