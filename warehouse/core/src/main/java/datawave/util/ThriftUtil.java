package datawave.util;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This code was repurposed from {@link org.apache.accumulo.core.rpc.ThriftUtil}.
 * It was not part of the public API, so we've created a DataWave equivalent.
 * This should be used in place of {@link org.apache.accumulo.core.rpc.ThriftUtil} moving forward.
 */
public class ThriftUtil {

    private static final Logger log = LoggerFactory.getLogger(datawave.util.ThriftUtil.class);

    /**
     * Close the client and return the transport to the pool.
     * @param client the client to close
     * @param context the client context
     */
    public static void close(TServiceClient client, ClientContext context) {
        if (client != null && client.getInputProtocol() != null && client.getInputProtocol().getTransport() != null) {
            context.getTransportPool().returnTransport(client.getInputProtocol().getTransport());
        } else {
            log.debug("Attempt to close null connection to a server", new Exception());
        }
    }
}
