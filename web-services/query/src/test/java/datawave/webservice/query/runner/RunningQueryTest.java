package datawave.webservice.query.runner;

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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.exception.EmptyObjectException;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.core.query.logic.ResponseEnricher;
import datawave.core.query.logic.composite.CompositeQueryLogic;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import datawave.webservice.query.logic.TestQueryLogic;
import datawave.webservice.query.logic.composite.CompositeQueryLogicTest;
import datawave.webservice.result.BaseQueryResponse;

public class RunningQueryTest {

    class SampleGenericQueryConfiguration extends GenericQueryConfiguration {
        // GenericQueryConfiguration is abstract. Looks like we are using the GenericShardQueryConfiguration
        // in the datawave-query package. This is not a dependency in maven though, but added from the lib. See
        // the setup() below. Here is an instantiatable class for testing.
        @SuppressWarnings("unused")
        private static final long serialVersionUID = -2981779719095867631L;
    }

    // variables common to all current tests
    private final QueryImpl settings = new QueryImpl();
    private final AccumuloConnectionFactory.Priority connectionPriority = AccumuloConnectionFactory.Priority.NORMAL;
    private String methodAuths = "";
    private SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDn", "issuerDn");
    private final QueryLogic<?> logic = createMock(BaseQueryLogic.class);

    @Before
    public void setup() throws MalformedURLException, IllegalArgumentException, IllegalAccessException {

        System.setProperty(DnUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");

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
    public void testConstructorSetsConnection() throws Exception {
        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));

        // setup mock connector
        InMemoryInstance instance = new InMemoryInstance("test instance");
        AccumuloClient client = new InMemoryAccumuloClient("root", instance);

        // setup mock logic, handles the setConnection method
        SampleGenericQueryConfiguration config = new SampleGenericQueryConfiguration();
        expect(logic.initialize(anyObject(), anyObject(), anyObject())).andReturn(config);
        logic.setupQuery(config);
        TransformIterator<?,?> iter = new TransformIterator<>();
        expect(logic.getCollectQueryMetrics()).andReturn(Boolean.FALSE);
        expect(logic.getTransformIterator(settings)).andReturn(iter);
        expect(logic.isLongRunningQuery()).andReturn(false);
        expect(logic.getResultLimit(settings)).andReturn(-1L);
        expect(logic.getMaxResults()).andReturn(-1L);
        logic.preInitialize(settings, AuthorizationsUtil.buildAuthorizations(null));
        expect(logic.getUserOperations()).andReturn(null);
        replay(logic);

        RunningQuery query = new RunningQuery(client, connectionPriority, logic, settings, methodAuths, principal, new QueryMetricFactoryImpl());

        verify(logic);

        // extra tests to verify setConnection worked. Would rather mock and don't really like multiple asserts per test, but there is too much setup
        assertEquals(client, query.getClient());
        assertEquals(iter, query.getTransformIterator());
    }

    @Test
    public void testConstructorWithNullConnector() throws Exception {
        AccumuloClient client = null;
        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));

        expect(logic.getCollectQueryMetrics()).andReturn(false);
        expect(logic.isLongRunningQuery()).andReturn(false);
        expect(logic.getResultLimit(settings)).andReturn(-1L);
        expect(logic.getMaxResults()).andReturn(-1L);
        logic.preInitialize(settings, AuthorizationsUtil.buildAuthorizations(null));
        expect(logic.getUserOperations()).andReturn(null);
        replay(logic);

        RunningQuery query = new RunningQuery(client, connectionPriority, logic, settings, methodAuths, principal, new QueryMetricFactoryImpl());

        assertEquals(client, query.getClient());
    }

    @Test(expected = AuthorizationException.class)
    public void testConstructorShouldNotMergeAuths() throws Exception {
        // setup
        AccumuloClient client = null;
        methodAuths = "A,B,C";

        // expected merged auths
        String[] auths = new String[2];
        auths[0] = "A";
        auths[1] = "C";
        Authorizations expected = new Authorizations(auths);

        expect(logic.getCollectQueryMetrics()).andReturn(false);
        logic.preInitialize(settings, AuthorizationsUtil.buildAuthorizations(Collections.singleton(Sets.newHashSet("A", "B", "C"))));
        expect(logic.getUserOperations()).andReturn(null);
        replay(logic);

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        RunningQuery query = new RunningQuery(client, connectionPriority, logic, settings, methodAuths, principal, new QueryMetricFactoryImpl());

        assertEquals(expected, query.getCalculatedAuths());
    }

    @Test
    public void testWithCompositeQueryLogic() throws Exception {
        // setup
        InMemoryInstance instance = new InMemoryInstance("test instance");
        AccumuloClient client = new InMemoryAccumuloClient("root", instance);

        // expected merged auths
        String[] auths = new String[2];
        auths[0] = "A";
        auths[1] = "C";
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic<?> logic1 = new TestQueryLogic<>();
        HashSet<String> roles = new HashSet<>();
        roles.add("NONTESTROLE");
        logic1.setTableName("thisTable");
        logic1.setRequiredRoles(roles);
        CompositeQueryLogicTest.TestQueryLogic2 logic2 = new CompositeQueryLogicTest.TestQueryLogic2();
        HashSet<String> roles2 = new HashSet<>();
        roles2.add("NONTESTROLE");
        logic2.setTableName("thatTable");
        logic2.setRequiredRoles(roles2);
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);
        CompositeQueryLogic compositeQueryLogic = new CompositeQueryLogic();
        compositeQueryLogic.setQueryLogics(logics);

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));

        compositeQueryLogic.setCurrentUser(principal);
        try {
            RunningQuery query = new RunningQuery(client, connectionPriority, compositeQueryLogic, settings, null, principal, new QueryMetricFactoryImpl());
        } catch (NullPointerException npe) {
            Assert.fail("NullPointer encountered. This could be caused by configuration being null. Check logic.initialize() ");
        }
    }

    // this shows that updates to the query plan will back propagate to the metric
    @Test
    public void testQueryStringUpdate() throws Exception {
        InMemoryInstance instance = new InMemoryInstance("test instance");
        AccumuloClient client = new InMemoryAccumuloClient("root", instance);

        String[] auths = new String[] {"A", "B"};

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));

        RotatingPlanQueryLogic logic = new RotatingPlanQueryLogic(3);
        logic.setMaxPageSize(1);

        settings.setQuery("FOO == 'bar'");
        RunningQuery rq = new RunningQuery(client, connectionPriority, logic, settings, null, principal, new QueryMetricFactoryImpl());
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());
        ResultsPage page = rq.next();
        // transforms two pages, even though only returns one
        assertEquals("((page = '2') && ((page = '1') && ((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))))", rq.getMetric().getPlan());
        assertEquals(1, page.getResults().size());
        page = rq.next();
        // transforms the third page
        assertEquals("((page = '3') && ((page = '2') && ((page = '1') && ((setup = 'true') && ((initialize = 'true') && FOO == 'bar')))))",
                        rq.getMetric().getPlan());
        assertEquals(1, page.getResults().size());
        page = rq.next();
        // transforms nothing, but returns the third page
        assertEquals("((page = '3') && ((page = '2') && ((page = '1') && ((setup = 'true') && ((initialize = 'true') && FOO == 'bar')))))",
                        rq.getMetric().getPlan());
        assertEquals(1, page.getResults().size());
        page = rq.next();
        // neither transforms nor returns anything
        assertEquals("((page = '3') && ((page = '2') && ((page = '1') && ((setup = 'true') && ((initialize = 'true') && FOO == 'bar')))))",
                        rq.getMetric().getPlan());
        assertEquals(0, page.getResults().size());
    }

    @Test
    public void testEmptyOrNullUpdate() throws Exception {
        InMemoryInstance instance = new InMemoryInstance("test instance");
        AccumuloClient client = new InMemoryAccumuloClient("root", instance);

        String[] auths = new String[] {"A", "B"};

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));

        // send back no change
        RotatingPlanQueryLogic logic = new RotatingPlanQueryLogic(3, false);
        logic.setMaxPageSize(1);
        settings.setQuery("FOO == 'bar'");
        RunningQuery rq = new RunningQuery(client, connectionPriority, logic, settings, null, principal, new QueryMetricFactoryImpl());
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());
        rq.next();
        rq.next();
        rq.next();
        rq.next();
        // unchanged
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());

        // set a null string
        logic = new RotatingPlanQueryLogic(3, false, true, null);
        logic.setMaxPageSize(1);
        settings.setQuery("FOO == 'bar'");
        rq = new RunningQuery(client, connectionPriority, logic, settings, null, principal, new QueryMetricFactoryImpl());
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());
        rq.next();
        rq.next();
        rq.next();
        rq.next();
        // unchanged
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());

        // set an empty string
        logic = new RotatingPlanQueryLogic(3, false, true, "");
        logic.setMaxPageSize(1);
        settings.setQuery("FOO == 'bar'");
        rq = new RunningQuery(client, connectionPriority, logic, settings, null, principal, new QueryMetricFactoryImpl());
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());
        rq.next();
        rq.next();
        rq.next();
        rq.next();
        // unchanged
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());

        // prove the overwrite works
        logic = new RotatingPlanQueryLogic(3, false, true, "overwrite-works");
        logic.setMaxPageSize(1);
        settings.setQuery("FOO == 'bar'");
        rq = new RunningQuery(client, connectionPriority, logic, settings, null, principal, new QueryMetricFactoryImpl());
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());
        rq.next();
        rq.next();
        rq.next();
        rq.next();
        // unchanged
        assertEquals("overwrite-works", rq.getMetric().getPlan());
    }

    private static class RotatingPlanQueryLogic extends BaseQueryLogic {
        final private int pages;
        final private boolean updatePages;
        final private boolean usePageOverride;
        final private String pageOverride;

        private int count = 0;

        public RotatingPlanQueryLogic(int pages) {
            this(pages, true);
        }

        public RotatingPlanQueryLogic(int pages, boolean updatePages) {
            this(pages, updatePages, false, null);
        }

        public RotatingPlanQueryLogic(int pages, boolean updatePages, boolean usePageOverride, String pageOverride) {
            this.pages = pages;
            this.updatePages = updatePages;
            this.usePageOverride = usePageOverride;
            this.pageOverride = pageOverride;
        }

        @Override
        public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set runtimeQueryAuthorizations) throws Exception {
            getConfig().setQuery(settings);
            getConfig().setQueryString("((initialize = 'true') && " + settings.getQuery() + ")");
            return getConfig();
        }

        @Override
        public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
            String oldPlan = getConfig().getQueryString();
            getConfig().setQueryString("((setup = 'true') && " + oldPlan + ")");
            this.iterator = new Iterator() {
                @Override
                public boolean hasNext() {
                    return count < pages;
                }

                @Override
                public Object next() {
                    count++;
                    return new Object();
                }
            };
        }

        @Override
        public Object clone() {
            return new RotatingPlanQueryLogic(pages, updatePages, usePageOverride, pageOverride);
        }

        @Override
        public AccumuloConnectionFactory.Priority getConnectionPriority() {
            return null;
        }

        @Override
        public QueryLogicTransformer getTransformer(Query settings) {
            return new QueryLogicTransformer() {
                @Override
                public void setResponseEnricher(ResponseEnricher responseTransform) {

                }

                @Override
                public BaseQueryResponse createResponse(ResultsPage resultList) {
                    return null;
                }

                @Override
                public Object transform(Object input) throws EmptyObjectException {
                    if (updatePages) {
                        getConfig().setQueryString("((page = '" + count + "') && " + getConfig().getQueryString() + ")");
                    } else if (usePageOverride) {
                        getConfig().setQueryString(pageOverride);
                    }
                    return new Object();
                }

                @Override
                public void setQueryExecutionForPageStartTime(long queryExecutionForCurrentPageStartTime) {

                }
            };
        }

        @Override
        public Set<String> getOptionalQueryParameters() {
            return Set.of();
        }

        @Override
        public Set<String> getRequiredQueryParameters() {
            return Set.of();
        }

        @Override
        public Set<String> getExampleQueries() {
            return Set.of();
        }
    }
}
