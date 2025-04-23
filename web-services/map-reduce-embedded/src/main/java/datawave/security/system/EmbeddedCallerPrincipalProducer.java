package datawave.security.system;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Alternative;
import javax.enterprise.inject.Produces;
import javax.interceptor.Interceptor;

import org.apache.commons.codec.binary.Base64;

import datawave.security.authorization.DatawavePrincipal;

/**
 * Caller principal producer supplied just for Embedded mode (e.g., inside of MapReduce jars). This archive should not be included for normal web applications.
 */
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class EmbeddedCallerPrincipalProducer {
    private boolean initialized = false;
    private DatawavePrincipal callerPrincipal;

    @Produces
    @CallerPrincipal
    public DatawavePrincipal produceCallerPrincipal() {
        if (!initialized) {
            initializeCallerPrincipal();
            initialized = true;
        }
        return callerPrincipal;
    }

    private void initializeCallerPrincipal() {
        String encodedCallerPrincipal = System.getProperty("caller.principal");
        if (encodedCallerPrincipal == null) {
            throw new IllegalStateException("System property caller.principal must be set to a serialized, base64 encoded principal.");
        }
        byte[] decodedCallerPrincipal = Base64.decodeBase64(encodedCallerPrincipal);

        try (ByteArrayInputStream bais = new ByteArrayInputStream(decodedCallerPrincipal); ObjectInputStream ois = new ObjectInputStream(bais)) {
            callerPrincipal = (DatawavePrincipal) ois.readObject();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
