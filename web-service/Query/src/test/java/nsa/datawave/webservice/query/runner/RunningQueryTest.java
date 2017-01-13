package nsa.datawave.webservice.query.runner;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.ArrayList;

import nsa.datawave.security.authorization.DatawavePrincipal;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.QueryImpl;
import nsa.datawave.webservice.query.cache.QueryMetricFactoryImpl;
import nsa.datawave.webservice.query.configuration.GenericQueryConfiguration;
import nsa.datawave.webservice.query.logic.BaseQueryLogic;
import nsa.datawave.webservice.query.logic.DatawaveRoleManager;
import nsa.datawave.webservice.query.logic.QueryLogic;
import nsa.datawave.webservice.query.logic.TestQueryLogic;
import nsa.datawave.webservice.query.logic.composite.CompositeQueryLogic;
import nsa.datawave.webservice.query.logic.composite.CompositeQueryLogicTest;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections.iterators.TransformIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RunningQueryTest {
    
    class SampleGenericQueryConfiguration extends GenericQueryConfiguration {
        // GenericQueryConfiguration is abstract. Looks like we are using the GenericShardQueryConfiguration
        // in the nsa.datawave-query package. This is not a dependency in maven though, but added from the lib. See
        // the setup() below. Here is an instantiatable class for testing.
        @SuppressWarnings("unused")
        private static final long serialVersionUID = 1L;
    }
    
    // variables common to all current tests
    final private QueryImpl settings = new QueryImpl();
    final private AccumuloConnectionFactory.Priority connectionPriority = AccumuloConnectionFactory.Priority.NORMAL;
    private String methodAuths = "";
    private final Set<Set<String>> userAuths = new HashSet<>();
    private final DatawavePrincipal principal = new DatawavePrincipal("userDn<isserDn>");
    private final QueryLogic<?> logic = createMock(BaseQueryLogic.class);
    
    @Before
    public void setup() throws MalformedURLException, IllegalArgumentException, IllegalAccessException {
        
        settings.setQueryLogicName("testQueryLogic");
        settings.setQuery("FOO == BAR");
        settings.setQueryName("test");
        settings.setColumnVisibility("A&B");
        settings.setBeginDate(new Date());
        settings.setEndDate(new Date());
        settings.setQueryAuthorizations("NONE");
        settings.setExpirationDate(new Date());
        settings.setPagesize(10);
        settings.setId(UUID.randomUUID());
        
        // get the files in conf and the jars in the lib directory and add them
        // to the classpath
        // so we can run this test. See notes in pom.xml under the
        // copy-dependencies plugin
        List<URL> additionalURLs = new LinkedList<>();
        additionalURLs.add(new File("conf/").toURI().toURL());
        
        URL[] urlsArray = additionalURLs.toArray(new URL[additionalURLs.size()]);
        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        URLClassLoader urlClassloader = new URLClassLoader(urlsArray, currentClassLoader);
        Thread.currentThread().setContextClassLoader(urlClassloader);
        
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testConstructorSetsConnection() throws AccumuloException, AccumuloSecurityException, Exception {
        // setup mock connector
        MockInstance instance = new MockInstance("test instance");
        Connector connector = instance.getConnector("root", new PasswordToken(""));
        
        // setup mock logic, handles the setConnection method
        SampleGenericQueryConfiguration config = new SampleGenericQueryConfiguration();
        expect(logic.initialize((Connector) anyObject(), (Query) anyObject(), (Set<Authorizations>) anyObject())).andReturn(config);
        logic.setupQuery(config);
        TransformIterator iter = new TransformIterator();
        expect(logic.getCollectQueryMetrics()).andReturn(Boolean.FALSE);
        expect(logic.getTransformIterator(settings)).andReturn(iter);
        replay(logic);
        
        RunningQuery query = new RunningQuery(connector, connectionPriority, logic, settings, methodAuths, principal, new QueryMetricFactoryImpl());
        
        verify(logic);
        
        // extra tests to verify setConnection worked. Would rather mock and don't really like multiple asserts per test, but there is too much setup
        assertEquals(connector, query.getConnection());
        assertEquals(iter, query.getTransformIterator());
    }
    
    @Test
    public void testConstructorWithNullConnector() throws Exception {
        Connector connector = null;
        
        RunningQuery query = new RunningQuery(connector, connectionPriority, logic, settings, methodAuths, principal, new QueryMetricFactoryImpl());
        
        assertEquals(connector, query.getConnection());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorShouldNotMergeAuths() throws Exception {
        // setup
        Connector connector = null;
        methodAuths = "A,B,C";
        HashSet<String> set = new HashSet<>();
        set.add("A");
        set.add("C");
        set.add("D");
        userAuths.add(set);
        
        // expected merged auths
        String[] auths = new String[2];
        auths[0] = "A";
        auths[1] = "C";
        Authorizations expected = new Authorizations(auths);
        
        principal.setAuthorizations(principal.getUserDN(), Arrays.asList(auths));
        RunningQuery query = new RunningQuery(connector, connectionPriority, logic, settings, methodAuths, principal, new QueryMetricFactoryImpl());
        
        assertEquals(expected, query.getCalculatedAuths());
    }
    
    @Test
    public void testWithCompositeQueryLogic() throws Exception {
        // setup
        MockInstance instance = new MockInstance("test instance");
        Connector connector = instance.getConnector("root", new PasswordToken(""));
        
        // expected merged auths
        String[] auths = new String[2];
        auths[0] = "A";
        auths[1] = "C";
        List<BaseQueryLogic<?>> logics = new ArrayList<BaseQueryLogic<?>>();
        TestQueryLogic logic1 = new TestQueryLogic();
        HashSet<String> roles = new HashSet<>();
        roles.add("NONTESTROLE");
        logic1.setTableName("thisTable");
        logic1.setRoleManager(new DatawaveRoleManager(roles));
        CompositeQueryLogicTest.TestQueryLogic2 logic2 = new CompositeQueryLogicTest.TestQueryLogic2();
        HashSet<String> roles2 = new HashSet<>();
        roles2.add("NONTESTROLE");
        logic2.setTableName("thatTable");
        logic2.setRoleManager(new DatawaveRoleManager(roles2));
        logics.add(logic1);
        logics.add(logic2);
        CompositeQueryLogic compositeQueryLogic = new CompositeQueryLogic();
        compositeQueryLogic.setQueryLogics(logics);
        
        principal.setAuthorizations(principal.getUserDN(), Arrays.asList(auths));
        try {
            RunningQuery query = new RunningQuery(connector, connectionPriority, compositeQueryLogic, settings, null, principal, new QueryMetricFactoryImpl());
        } catch (NullPointerException npe) {
            Assert.fail("NullPointer encountered. This could be caused by configuration being null. Check logic.initialize() ");
        }
    }
}
