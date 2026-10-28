package datawave.webservice;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.result.ConnectionPool;

public class InMemoryAccumuloConnectionFactoryImpl implements AccumuloConnectionFactory {
    private final InMemoryInstance inMemoryInstance = new InMemoryInstance();

    public InMemoryAccumuloConnectionFactoryImpl(String username, String authorizations) {
        try (AccumuloClient client = new InMemoryAccumuloClient(username, inMemoryInstance)) {
            String[] authArray = Arrays.stream(authorizations.split(",")).toArray(String[]::new);
            client.securityOperations().changeUserAuthorizations(client.whoami(), new Authorizations(authArray));
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AccumuloClient getClient(String userDN, Collection<String> proxiedDNs, Priority priority, Map<String,String> trackingMap) throws Exception {
        return new InMemoryAccumuloClient("root", inMemoryInstance);
    }

    @Override
    public AccumuloClient getClient(String userDN, Collection<String> proxiedDNs, String poolName, Priority priority, Map<String,String> trackingMap)
                    throws Exception {
        return new InMemoryAccumuloClient("root", inMemoryInstance);
    }

    @Override
    public void returnClient(AccumuloClient client) {

    }

    @Override
    public String report() {
        return null;
    }

    @Override
    public List<ConnectionPool> getConnectionPools() {
        return null;
    }

    @Override
    public int getConnectionUsagePercent() {
        return 0;
    }

    @Override
    public Map<String,String> getTrackingMap(StackTraceElement[] stackTrace) {
        return new HashMap<>();
    }

    @Override
    public void close() throws Exception {

    }
}
