package datawave.webservice.query.limit;

import java.net.UnknownHostException;

/**
 * Interface defining methods to return hostname information. While calls can be made directly to InetAddress, usage of this interface allows for easier mocking
 * and injection for testing purposes.
 */
public interface HostnameProvider {

    static InetHostnameProvider getInetAddressProvider() {
        return new InetHostnameProvider();
    }

    /**
     * Return the canonical hostname.
     *
     * @return the hostname
     * @throws UnknownHostException
     *             if the hostname cannot be resolved
     */
    String getCanonicalHostname() throws UnknownHostException;
}
