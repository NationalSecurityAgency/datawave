package datawave.security.authorization.test;

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

import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.SubjectIssuerDNPair;

@ExtendWith(ArquillianExtension.class)
public class TestWithOnlyNonCachedAlternativesTest extends TestDatawaveUserServiceTest {
    @Inject
    private DatawaveUserService userService;

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        System.setProperty("cdi.bean.context", "testAuthServiceBeanRefContext.xml");
        System.setProperty("dw.security.use.testuserservice", "true");
        return ShrinkWrap.create(JavaArchive.class).addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi")
                        .addClasses(TestDatawaveUserService.class, DatawaveUserService1.class, AltDatawaveUserService1.class, AltDatawaveUserService2.class,
                                        MockAccumuloConnectionFactory.class)
                        .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
    }

    @Test
    public void testCorrectAlternative() throws Exception {
        DatawaveUser user = userService.lookup(Collections.singleton(SubjectIssuerDNPair.of("subject1", "issuer1"))).iterator().next();
        assertTrue(user.getRoles().contains("Alternative2"));
    }

    @Test
    public void testCannedUser() throws Exception {
        DatawaveUser user = userService.lookup(Collections.singleton(SubjectIssuerDNPair.of("cn=server1, ou=my department, o=my company, st=some-state, c=us",
                        "cn=my company ca, o=my company, st=some-state, c=us"))).iterator().next();
        assertTrue(user.getAuths().contains("PUB"));
        assertTrue(user.getAuths().contains("PVT"));
    }
}
