package datawave.security.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;

import datawave.security.util.SecurityConstants;

/**
 * Tests for {@link DatawaveHttpAuthenticationMechanism.Config}.
 */
public class DatawaveHttpAuthenticationMechanismConfigTests {

    /**
     * Verify the default values expected from {@link DatawaveHttpAuthenticationMechanism.Config#fromMap(Map)} when no system properties are configured with
     * trusted headers.
     */
    @ClearSystemProperty(key = SecurityConstants.TRUSTED_SUBJECT_DN_HEADER_SYSTEM_PROPERTY)
    @ClearSystemProperty(key = SecurityConstants.TRUSTED_ISSUER_DN_HEADER_SYSTEM_PROPERTY)
    @Test
    void testDefaultValuesGivenNoSystemPropertiesSet() {
        DatawaveHttpAuthenticationMechanism.Config config = DatawaveHttpAuthenticationMechanism.Config.fromMap(Map.of());
        assertThat(config.getTrustedSubjectDnHeader()).isEqualTo(SecurityConstants.DEFAULT_TRUSTED_SUBJECT_DN_HEADER);
        assertThat(config.getTrustedIssuerDnHeader()).isEqualTo(SecurityConstants.DEFAULT_TRUSTED_ISSUER_DN_HEADER);
        assertThat(config.isIdentityRestorationEnabled()).isTrue();
        assertThat(config.isSessionIdChangeEnabled()).isTrue();
    }

    /**
     * Verify the values expected from {@link DatawaveHttpAuthenticationMechanism.Config#fromMap(Map)} when system properties are configured with trusted
     * headers.
     */
    @SetSystemProperty(key = SecurityConstants.TRUSTED_SUBJECT_DN_HEADER_SYSTEM_PROPERTY, value = "Alt-TrustedSubject")
    @SetSystemProperty(key = SecurityConstants.TRUSTED_ISSUER_DN_HEADER_SYSTEM_PROPERTY, value = "Alt-TrustedIssuer")
    @Test
    void testDefaultValuesGivenSystemPropertiesSet() {
        DatawaveHttpAuthenticationMechanism.Config config = DatawaveHttpAuthenticationMechanism.Config.fromMap(Map.of());
        assertThat(config.getTrustedSubjectDnHeader()).isEqualTo("Alt-TrustedSubject");
        assertThat(config.getTrustedIssuerDnHeader()).isEqualTo("Alt-TrustedIssuer");
        assertThat(config.isIdentityRestorationEnabled()).isTrue();
        assertThat(config.isSessionIdChangeEnabled()).isTrue();
    }

    /**
     * Verify that when options are configured via the property map, they override everything else, including system properties.
     */
    @SetSystemProperty(key = SecurityConstants.TRUSTED_SUBJECT_DN_HEADER_SYSTEM_PROPERTY, value = "Alt-TrustedSubject")
    @SetSystemProperty(key = SecurityConstants.TRUSTED_ISSUER_DN_HEADER_SYSTEM_PROPERTY, value = "Alt-TrustedIssuer")
    @Test
    void testValuesGivenOverrides() {
        Map<String,Object> properties = new HashMap<>();
        properties.put(DatawaveHttpAuthenticationMechanism.Config.OPTION_TRUSTED_SUBJECT_DN_HEADER, "Override-TrustedSubject");
        properties.put(DatawaveHttpAuthenticationMechanism.Config.OPTION_TRUSTED_ISSUER_DN_HEADER, "Override-TrustedIssuer");
        properties.put(DatawaveHttpAuthenticationMechanism.Config.OPTION_ENABLE_RESTORE_IDENTITY, "false");
        properties.put(DatawaveHttpAuthenticationMechanism.Config.OPTION_ENABLE_SESSION_ID_CHANGE, "false");

        DatawaveHttpAuthenticationMechanism.Config config = DatawaveHttpAuthenticationMechanism.Config.fromMap(properties);
        assertThat(config.getTrustedSubjectDnHeader()).isEqualTo("Override-TrustedSubject");
        assertThat(config.getTrustedIssuerDnHeader()).isEqualTo("Override-TrustedIssuer");
        assertThat(config.isIdentityRestorationEnabled()).isFalse();
        assertThat(config.isSessionIdChangeEnabled()).isFalse();
    }
}
