package datawave.springframework.integration;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.jms.JMSContext;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.easymock.EasyMock;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.result.ConnectionPool;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.system.CallerPrincipal;

@ApplicationScoped
public class TestProducer {
    private static final ExecutorService mockExecutorService = EasyMock.createMock(ExecutorService.class);
    private static DatawavePrincipal mockDatawavePrincipal = EasyMock.createMock(DatawavePrincipal.class);
    private static JMSContext mockJMSContext = EasyMock.createMock(JMSContext.class);

    @Produces
    public static JMSContext produceJMSContext() {
        return mockJMSContext;
    }

    @Produces
    @CallerPrincipal
    @RequestScoped
    public DatawavePrincipal produceCallerPrincipal() {
        return mockDatawavePrincipal;
    }

    @Produces
    public static ExecutorService produceExecutorService() {
        return mockExecutorService;
    }

    @Produces
    public static AccumuloConnectionFactory produceAccumuloConnectionFactory() {
        return new MockAccumuloConnectionFactory("root", "PVT,PUBLIC");
    }

    private static class MockAccumuloConnectionFactory implements AccumuloConnectionFactory {
        private InMemoryInstance inMemoryInstance = new InMemoryInstance();

        public MockAccumuloConnectionFactory(String username, String authorizations) {
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
}
