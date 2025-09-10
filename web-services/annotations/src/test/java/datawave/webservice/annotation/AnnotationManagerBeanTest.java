package datawave.webservice.annotation;

import static org.powermock.api.easymock.PowerMock.createStrictMock;
import static org.powermock.reflect.Whitebox.setInternalState;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.ejb.EJBContext;
import javax.ws.rs.core.Response;

import datawave.webservice.query.exception.QueryException;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.jboss.resteasy.core.Dispatcher;
import org.jboss.resteasy.mock.MockDispatcherFactory;
import org.jboss.resteasy.mock.MockHttpRequest;
import org.jboss.resteasy.mock.MockHttpResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.user.UserOperationsBean;
import datawave.webservice.query.configuration.LookupUUIDConfiguration;
import datawave.webservice.query.logic.QueryLogicFactoryImpl;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.runner.AccumuloConnectionRequestBean;
import datawave.webservice.query.runner.QueryExecutor;

@RunWith(PowerMockRunner.class)

@PowerMockIgnore({"java.*", "javax.*", "com.*", "org.apache.*", "org.w3c.*", "net.sf.*"})
public class AnnotationManagerBeanTest {

    private static final Logger log = Logger.getLogger(AnnotationManagerBeanTest.class);

    private final String userDN = "CN=Guy Some Other soguy, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US";
    private final String[] auths = new String[] {"PRIVATE", "PUBLIC"};

    // AnnotationManagerBean dependencies
    private EJBContext ctx;
    private AccumuloConnectionFactory connectionFactory;
    private QueryExecutor queryExecutor;
    private QueryLogicFactory queryLogicFactory;
    private ResponseObjectFactory responseObjectFactory;
    private UserOperationsBean userOperationsBean;
    private AccumuloConnectionRequestBean connectionRequestBean;
    private LookupUUIDConfiguration lookupUUIDConfiguration;

    // RESTEasy Stuff
    private Dispatcher dispatcher;
    private MockHttpRequest request;
    private MockHttpResponse response;

    private AnnotationManagerBean bean;

    @Before
    public void setup() throws Exception {
        bean = new AnnotationManagerBean();

        ctx = createStrictMock(EJBContext.class);
        connectionFactory = createStrictMock(AccumuloConnectionFactory.class);

        queryLogicFactory = createStrictMock(QueryLogicFactoryImpl.class);
        responseObjectFactory = createStrictMock(ResponseObjectFactory.class);
        connectionRequestBean = createStrictMock(AccumuloConnectionRequestBean.class);

        setInternalState(connectionRequestBean, EJBContext.class, ctx);

        setInternalState(bean, EJBContext.class, ctx);
        setInternalState(bean, AccumuloConnectionFactory.class, connectionFactory);

        setInternalState(bean, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(bean, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(bean, AccumuloConnectionRequestBean.class, connectionRequestBean);

        // RESTEasy mock stuff
        dispatcher = MockDispatcherFactory.createDispatcher();
        dispatcher.getRegistry().addSingletonResource(bean, "/DataWave/Annotations/v1");
        response = new MockHttpResponse();
    }

    @Test
    public void testGetAll() throws Exception {
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "<CN=MY_CA, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US>"),
                        DatawaveUser.UserType.USER, Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        String[] dns = principal.getDNs();
        Arrays.sort(dns);
        List<String> dnList = Arrays.asList(dns);

        PowerMock.resetAll();

        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal).anyTimes();

        PowerMock.replayAll();

        Response response = bean.getAnnotationsFor("DOCUMENT", "20250406_456/news/aiddza.kdn85e.-wnbwkq");

        PowerMock.verifyAll();
    }
}
