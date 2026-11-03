package datawave.security.authorization.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService1;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService2;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService3;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService4;
import datawave.security.authorization.test.AlternateDatawaveUserServices.AltDatawaveUserService5;
import datawave.security.authorization.test.AlternateDatawaveUserServices.DatawaveUserService1;
import datawave.webservice.config.TestDatawaveUserProperties;
import datawave.webservice.config.WebserviceConfiguration;

@ExtendWith(ArquillianExtension.class)
public class TestDatawaveUserServiceDisabledTest extends TestDatawaveUserServiceTest {
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
                                        AltDatawaveUserService3.class, AltDatawaveUserService4.class, AltDatawaveUserService5.class,
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
