package datawave.webservice.query.limit;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Default implementation of {@link HostnameProvider}.
 */
public class InetHostnameProvider implements HostnameProvider {

    private String canonicalHostname;

    @Override
    public String getCanonicalHostname() throws UnknownHostException {
        if (canonicalHostname == null) {
            canonicalHostname = InetAddress.getLocalHost().getCanonicalHostName();
        }
        return canonicalHostname;
    }
}
