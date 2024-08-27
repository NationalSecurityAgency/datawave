package datawave.util;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftUtil{

    private static final Logger log = LoggerFactory.getLogger(datawave.util.ThriftUtil.class);

    public static void close(TServiceClient client, ClientContext context) {
        if (client != null && client.getInputProtocol() != null
                && client.getInputProtocol().getTransport() != null) {
            context.getTransportPool().returnTransport(client.getInputProtocol().getTransport());
        } else {
            log.debug("Attempt to close null connection to a server", new Exception());
        }
    }
}