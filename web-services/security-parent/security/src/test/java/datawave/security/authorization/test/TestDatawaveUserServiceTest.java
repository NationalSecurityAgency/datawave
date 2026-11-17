package datawave.security.authorization.test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.result.ConnectionPool;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.DatawaveUserInfo;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.SubjectIssuerDNPair;

public class TestDatawaveUserServiceTest {

    protected static class DatawaveUserService1 extends DefaultDatawaveUserService {
        @Override
        String getName() {
            return "DefaultService";
        }
    }

    @Alternative
    @Priority(1)
    protected static class AltDatawaveUserService1 extends DefaultDatawaveUserService {
        @Override
        String getName() {
            return "Alternative1";
        }
    }

    @Alternative
    @Priority(2)
    protected static class AltDatawaveUserService2 extends DefaultDatawaveUserService {
        @Override
        String getName() {
            return "Alternative2";
        }
    }

    @Alternative
    @Priority(3)
    protected static class AltDatawaveUserService3 extends DefaultCachedDatawaveUserService {
        @Override
        String getName() {
            return "Alternative3";
        }
    }

    @Alternative
    @Priority(4)
    protected static class AltDatawaveUserService4 extends DefaultCachedDatawaveUserService {
        @Override
        String getName() {
            return "Alternative4";
        }
    }

    @Alternative
    @Priority(5)
    protected static class AltDatawaveUserService5 extends DefaultDatawaveUserService {
        @Override
        String getName() {
            return "Alternative5";
        }
    }

    protected abstract static class DefaultDatawaveUserService implements DatawaveUserService {
        abstract String getName();

        @Override
        public Collection<DatawaveUser> lookup(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
            return dns.stream().map(dn -> new DatawaveUser(dn, UserType.USER, null, null, Collections.singleton(getName()), null, -1L, -1L))
                            .collect(Collectors.toList());
        }
    }

    protected abstract static class DefaultCachedDatawaveUserService extends DefaultDatawaveUserService implements CachedDatawaveUserService {

        @Override
        public Collection<DatawaveUser> reload(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
            return null;
        }

        @Override
        public DatawaveUser list(String name) {
            return null;
        }

        @Override
        public Collection<? extends DatawaveUserInfo> listAll() {
            return null;
        }

        @Override
        public Collection<? extends DatawaveUserInfo> listMatching(String substring) {
            return null;
        }

        @Override
        public String evict(String name) {
            return null;
        }

        @Override
        public String evictMatching(String substring) {
            return null;
        }

        @Override
        public String evictAll() {
            return null;
        }
    }

    protected static class MockAccumuloConnectionFactory implements AccumuloConnectionFactory {
        private InMemoryInstance inMemoryInstance = new InMemoryInstance();

        public MockAccumuloConnectionFactory() {
            try {
                new InMemoryAccumuloClient("root", inMemoryInstance).securityOperations().changeUserAuthorizations("root", new Authorizations("PUB", "PVT"));
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
