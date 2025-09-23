package datawave.security.authorization.remote;

import static datawave.webservice.common.remote.RemoteServiceUtil.NON_ROUTABLE_HOST;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

import datawave.authorization.remote.RemoteAuthorizationException;
import datawave.common.test.integration.IntegrationTest;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;

@Category(IntegrationTest.class)
public class RemoteUserOperationsImplIT {
    private RemoteUserOperationsUtil remoteUtil;
    private RemoteUserOperationsImpl userOperations;
    private DatawavePrincipal datawavePrincipal;

    @Before
    public void setup() throws IOException, CertificateException, NoSuchAlgorithmException {
        remoteUtil = new RemoteUserOperationsUtil();
        remoteUtil.initialize();

        userOperations = remoteUtil.getUserOperations();
        // don't retry for this test
        userOperations.setRetryCount(0);

        SubjectIssuerDNPair dn = SubjectIssuerDNPair.of("userDn", "issuerDn");
        DatawaveUser user = new DatawaveUser(dn, DatawaveUser.UserType.USER, Sets.newHashSet(), null, null, -1L);
        datawavePrincipal = new DatawavePrincipal(Collections.singleton(user));
    }

    @After
    public void cleanup() {
        remoteUtil.stop();
    }

    @Test(expected = RemoteAuthorizationException.class)
    public void testSocketTimeout() throws AuthorizationException {
        userOperations.setSocketTimeout(100);

        userOperations.listEffectiveAuthorizations(datawavePrincipal);
    }

    @Test(expected = RemoteAuthorizationException.class)
    public void testConnectTimeout() throws AuthorizationException {
        userOperations.setQueryServiceHost(NON_ROUTABLE_HOST);

        userOperations.setConnectTimeout(100);

        userOperations.listEffectiveAuthorizations(datawavePrincipal);
    }

    @Test
    public void testConnectionPoolTimeout() throws AuthorizationException {
        userOperations.setConnectionPoolTimeout(100);
        userOperations.setMaxConnections(1);

        remoteUtil.updateRoute("/Security/User/listEffectiveAuthorizations", exchange -> {
            try {
                // this call is triggered by the first. kickoff a second one that will be blocked by the connection pool being full after 100ms
                userOperations.listEffectiveAuthorizations(datawavePrincipal);
                fail();
            } catch (AuthorizationException e) {
                // this is thrown due to the connectionPoolTimeout
                assertTrue(e instanceof RemoteAuthorizationException);
            }

            remoteUtil.getEmptyResponseHandler().handle(exchange);
        });

        // trigger the first call which will use the above handler to process the second call
        userOperations.listEffectiveAuthorizations(datawavePrincipal);
    }
}
