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
 * Server principal producer supplied just for Embedded mode (e.g., inside of MapReduce jars). This archive should not be included for normal web applications.
 */
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class EmbeddedServerPrincipalProducer {
    private boolean initialized = false;
    private DatawavePrincipal serverPrincipal;

    @Produces
    @ServerPrincipal
    public DatawavePrincipal produceServerPrincipal() {
        if (!initialized) {
            initializeServerPrincipal();
            initialized = true;
        }
        return serverPrincipal;
    }

    private void initializeServerPrincipal() {
        String encodedServerPrincipal = System.getProperty("server.principal");
        if (encodedServerPrincipal == null) {
            throw new IllegalStateException("System property server.principal must be set to a serialized, base64 encoded principal.");
        }
        byte[] decodedServerPrincipal = Base64.decodeBase64(encodedServerPrincipal);

        try (ByteArrayInputStream bais = new ByteArrayInputStream(decodedServerPrincipal); ObjectInputStream ois = new ObjectInputStream(bais)) {
            serverPrincipal = (DatawavePrincipal) ois.readObject();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
