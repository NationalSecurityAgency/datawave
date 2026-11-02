package datawave.security.authorization.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;

import javax.inject.Inject;

import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.SubjectIssuerDNPair;

@ExtendWith(ArquillianExtension.class)
public class TestDatawaveUserServiceDisabledTest extends TestDatawaveUserServiceTest {
    @Inject
    private DatawaveUserService userService;

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        System.setProperty("cdi.bean.context", "testAuthServiceBeanRefContext.xml");
        System.setProperty("dw.security.use.testuserservice", "false");
        return ShrinkWrap.create(JavaArchive.class).addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi")
                        .addClasses(TestDatawaveUserService.class, DatawaveUserService1.class, AltDatawaveUserService1.class, AltDatawaveUserService2.class)
                        .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
    }

    @Test
    public void testCorrectAlternative() throws Exception {
        assertEquals(AltDatawaveUserService2.class, userService.getClass());
        DatawaveUser user = userService.lookup(Collections.singleton(SubjectIssuerDNPair.of("subject1", "issuer1"))).iterator().next();
        assertEquals("Alternative2", user.getRoles().iterator().next());
    }
}
