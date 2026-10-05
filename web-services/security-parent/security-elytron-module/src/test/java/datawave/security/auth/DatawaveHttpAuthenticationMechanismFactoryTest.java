package datawave.security.auth;

import static datawave.security.auth.DatawaveHttpAuthenticationMechanismFactory.DATAWAVE_AUTH_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import javax.security.auth.callback.CallbackHandler;

import org.junit.jupiter.api.Test;
import org.wildfly.security.http.HttpServerAuthenticationMechanism;

class DatawaveHttpAuthenticationMechanismFactoryTest {

    /**
     * Verify that {@link DatawaveHttpAuthenticationMechanismFactory#getMechanismNames(Map)} returns an array containing only
     * {@value DatawaveHttpAuthenticationMechanismFactory#DATAWAVE_AUTH_NAME}.
     */
    @Test
    void testGetMechanismNames() {
        DatawaveHttpAuthenticationMechanismFactory factory = new DatawaveHttpAuthenticationMechanismFactory();
        String[] mechanismNames = factory.getMechanismNames(Map.of());
        assertThat(mechanismNames).containsExactly(DATAWAVE_AUTH_NAME);
    }

    /**
     * Verify that when {@link DatawaveHttpAuthenticationMechanismFactory#createAuthenticationMechanism(String, Map, CallbackHandler)} is called with the
     * mechanism {@value DatawaveHttpAuthenticationMechanismFactory#DATAWAVE_AUTH_NAME}, a {@link DatawaveHttpAuthenticationMechanism} is returned.
     */
    @Test
    void testCreateAuthenticationMechanismGivenDatawaveAuthMechanism() {
        CallbackHandler callbackHandler = callbacks -> {};
        DatawaveHttpAuthenticationMechanismFactory factory = new DatawaveHttpAuthenticationMechanismFactory();
        HttpServerAuthenticationMechanism authenticationMechanism = factory.createAuthenticationMechanism(DATAWAVE_AUTH_NAME, Map.of(), callbackHandler);
        assertThat(authenticationMechanism).isInstanceOf(DatawaveHttpAuthenticationMechanism.class);
    }

    /**
     * Verify that when {@link DatawaveHttpAuthenticationMechanismFactory#createAuthenticationMechanism(String, Map, CallbackHandler)} is called with a
     * mechanism other than {@value DatawaveHttpAuthenticationMechanismFactory#DATAWAVE_AUTH_NAME}, null is returned.
     */
    @Test
    void testCreateAuthenticationMechanismGivenNonDatawaveAuthMechanism() {
        CallbackHandler callbackHandler = callbacks -> {};
        DatawaveHttpAuthenticationMechanismFactory factory = new DatawaveHttpAuthenticationMechanismFactory();
        HttpServerAuthenticationMechanism authenticationMechanism = factory.createAuthenticationMechanism("BASIC", Map.of(), callbackHandler);
        assertThat(authenticationMechanism).isNull();
    }
}
