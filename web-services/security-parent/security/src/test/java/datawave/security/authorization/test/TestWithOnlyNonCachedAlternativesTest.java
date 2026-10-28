package datawave.security.authorization.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;

import javax.inject.Inject;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import datawave.configuration.spring.SpringBean;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService1;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService2;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService3;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService4;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService5;
import datawave.security.authorization.test.AlternateDatawaveUserServices.DatawaveUserService1;
import datawave.webservice.config.RemoteServicesConfiguration;
import datawave.webservice.config.TestDatawaveUserProperties;

@ExtendWith(ArquillianExtension.class)
public class TestWithOnlyNonCachedAlternativesTest extends TestDatawaveUserServiceTest {
    @Inject
    @SpringBean(name = "datawaveUserService")
    private DatawaveUserService userService;

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        System.setProperty("datawave.configuration.spring.useBootstrapContext", "true");
        System.setProperty("spring.cloud.config.enabled", "false");
        System.setProperty("cdi.bean.context", "testAuthServiceBeanRefContext.xml");

        String keyStoreUrl = TestDatawaveUserService.class.getClassLoader().getResource("datawave-server-keystore.p12").toURI().toString();
        String trustStoreUrl = TestDatawaveUserService.class.getClassLoader().getResource("datawave-server-truststore.jks").toURI().toString();
        System.setProperty("server.ssl.key-store", keyStoreUrl);
        System.setProperty("server.ssl.trust-store", trustStoreUrl);
        System.setProperty("datawave.active-implementation.datawave-user-service", AltDatawaveUserService2.class.getName());
        return ShrinkWrap.create(JavaArchive.class).addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi")
                        .addClasses(TestDatawaveUserService.class, DatawaveUserService1.class, AltDatawaveUserService1.class, AltDatawaveUserService2.class,
                                        AltDatawaveUserService3.class, AltDatawaveUserService4.class, AltDatawaveUserService5.class,
                                        MockAccumuloConnectionFactory.class, RemoteServicesConfiguration.class, TestDatawaveUserProperties.class)
                        .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
    }

    @Test
    public void testCorrectAlternative() throws Exception {
        assertFalse(userService instanceof CachedDatawaveUserService);
    }

    @Test
    public void testCannedUser() throws Exception {
        DatawaveUser user = userService.lookup(Collections.singleton(serverDN)).iterator().next();
        assertTrue(user.getRoles().contains("Alternative2"));
    }
}
