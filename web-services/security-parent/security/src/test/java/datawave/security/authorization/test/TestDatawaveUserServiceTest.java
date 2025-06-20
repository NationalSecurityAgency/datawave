package datawave.security.authorization.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.configuration.spring.SpringBean;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.result.ConnectionPool;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService1;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService2;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService3;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService4;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService5;
import datawave.security.authorization.test.AlternateDatawaveUserServices.DatawaveUserService1;
import datawave.webservice.config.TestDatawaveUserProperties;
import datawave.webservice.config.WebserviceConfiguration;

@RunWith(Enclosed.class)
public class TestDatawaveUserServiceTest {

    private static SubjectIssuerDNPair serverDN = SubjectIssuerDNPair.of("cn=server1, ou=my department, o=my company, st=some-state, c=us",
                    "cn=my company ca, o=my company, st=some-state, c=us");

    @RunWith(Arquillian.class)
    public static class TestDatawaveUserServiceDisabled {
        @Inject
        @SpringBean(name = "datawaveUserService")
        private DatawaveUserService userService;

        @Deployment
        public static JavaArchive createDeployment() throws Exception {
            System.setProperty("datawave.configuration.spring.useBootstrapContext", "true");
            System.setProperty("spring.cloud.config.enabled", "false");
            System.setProperty("cdi.bean.context", "testAuthServiceBeanRefContext.xml");
            System.setProperty("dw.security.activeService", "ALT_USER_SERVICE2");
            return ShrinkWrap.create(JavaArchive.class).addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi")
                            .addClasses(TestDatawaveUserService.class, DatawaveUserService1.class, AltDatawaveUserService1.class, AltDatawaveUserService2.class,
                                            MockAccumuloConnectionFactory.class, WebserviceConfiguration.class, TestDatawaveUserProperties.class)
                            .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
        }

        @Test
        public void testCorrectAlternative() throws Exception {
            assertEquals(AltDatawaveUserService2.class, userService.getClass());
            DatawaveUser user = userService.lookup(Collections.singleton(serverDN)).iterator().next();
            assertTrue(user.getRoles().contains("Alternative2"));
        }
    }

    @RunWith(Arquillian.class)
    public static class TestWithOnlyNonCachedAlternatives {
        @Inject
        @SpringBean(name = "datawaveUserService")
        private DatawaveUserService userService;

        @Deployment
        public static JavaArchive createDeployment() throws Exception {
            System.setProperty("datawave.configuration.spring.useBootstrapContext", "true");
            System.setProperty("spring.cloud.config.enabled", "false");
            System.setProperty("cdi.bean.context", "testAuthServiceBeanRefContext.xml");
            System.setProperty("dw.security.activeService", "ALT_USER_SERVICE2");
            return ShrinkWrap.create(JavaArchive.class).addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi")
                            .addClasses(TestDatawaveUserService.class, DatawaveUserService1.class, AltDatawaveUserService1.class, AltDatawaveUserService2.class,
                                            MockAccumuloConnectionFactory.class, WebserviceConfiguration.class, TestDatawaveUserProperties.class)
                            .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
        }

        @Test
        public void testCorrectAlternative() throws Exception {
            DatawaveUser user = userService.lookup(Collections.singleton(serverDN)).iterator().next();
            assertTrue(user.getRoles().contains("Alternative2"));
        }

        @Test
        public void testCannedUser() throws Exception {
            DatawaveUser user = userService.lookup(Collections.singleton(serverDN)).iterator().next();
            assertTrue(user.getAuths().contains("PUB"));
            assertTrue(user.getAuths().contains("PVT"));
        }

    }

    @RunWith(Arquillian.class)
    public static class TestWithOnlCachedAlternatives {
        @Inject
        @SpringBean(name = "datawaveUserService")
        private DatawaveUserService userService;

        @Deployment
        public static JavaArchive createDeployment() throws Exception {
            System.setProperty("datawave.configuration.spring.useBootstrapContext", "true");
            System.setProperty("spring.cloud.config.enabled", "false");
            System.setProperty("cdi.bean.context", "testAuthServiceBeanRefContext.xml");
            System.setProperty("dw.security.activeService", "ALT_USER_SERVICE4");
            return ShrinkWrap.create(JavaArchive.class).addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi")
                            .addClasses(TestDatawaveUserService.class, DatawaveUserService1.class, AltDatawaveUserService1.class, AltDatawaveUserService2.class,
                                            AltDatawaveUserService3.class, AltDatawaveUserService4.class, AltDatawaveUserService5.class,
                                            MockAccumuloConnectionFactory.class, WebserviceConfiguration.class, TestDatawaveUserProperties.class)
                            .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
        }

        @Test
        public void testCorrectAlternative() throws Exception {
            DatawaveUser user = userService.lookup(Collections.singleton(serverDN)).iterator().next();
            assertTrue(user.getRoles().contains("Alternative4"));
        }

        @Test
        public void testCannedUser() throws Exception {
            DatawaveUser user = userService.lookup(Collections.singleton(serverDN)).iterator().next();
            assertTrue(user.getAuths().contains("PUB"));
            assertTrue(user.getAuths().contains("PVT"));
        }

    }

    private static class MockAccumuloConnectionFactory implements AccumuloConnectionFactory {
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
