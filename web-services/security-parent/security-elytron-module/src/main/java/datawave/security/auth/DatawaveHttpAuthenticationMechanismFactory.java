package datawave.security.auth;

import java.util.Map;
import java.util.Objects;

import javax.security.auth.callback.CallbackHandler;

import org.wildfly.security.http.HttpServerAuthenticationMechanism;
import org.wildfly.security.http.HttpServerAuthenticationMechanismFactory;

/**
 * A {@link HttpServerAuthenticationMechanismFactory} implementation to create instances of {@link DatawaveHttpAuthenticationMechanism} for the
 * {@value #DATAWAVE_AUTH_NAME} mechanism.
 */
public class DatawaveHttpAuthenticationMechanismFactory implements HttpServerAuthenticationMechanismFactory {

    public static final String DATAWAVE_AUTH_NAME = "DATAWAVE-AUTH";

    /**
     * Returns the name of the HTTP authentication mechanism that can be supplied by this factory, specifically {@value #DATAWAVE_AUTH_NAME}.
     *
     * @param properties
     *            the properties to pass configuration to the mechanisms that may be evaluated for mechanism availability.
     * @return a single-element array containing the string {@value #DATAWAVE_AUTH_NAME}.
     */
    @Override
    public String[] getMechanismNames(Map<String,?> properties) {
        return new String[] {DATAWAVE_AUTH_NAME};
    }

    /**
     * Returns an instance of {@link DatawaveHttpAuthenticationMechanism} if the mechanism name is {@value #DATAWAVE_AUTH_NAME}, otherwise returns null.
     *
     * @param mechanismName
     *            the mechanism name
     * @param properties
     *            the set of properties to select and configure the mechanism that may be evaluated for mechanism availability
     * @param callbackHandler
     *            the {@link CallbackHandler} for use by the mechanism during authentication
     * @return the configured {@link DatawaveHttpAuthenticationMechanism} or null if no mechanism could be resolved for the given mechanism name
     */
    @Override
    public HttpServerAuthenticationMechanism createAuthenticationMechanism(String mechanismName, Map<String,?> properties, CallbackHandler callbackHandler) {
        Objects.requireNonNull(mechanismName, "mechanismName must not be null");
        Objects.requireNonNull(properties, "properties must not be null");
        Objects.requireNonNull(callbackHandler, "callbackHandler must not be null");

        if (DATAWAVE_AUTH_NAME.equals(mechanismName)) {
            return new DatawaveHttpAuthenticationMechanism(properties, callbackHandler);
        } else {
            return null;
        }
    }
}
