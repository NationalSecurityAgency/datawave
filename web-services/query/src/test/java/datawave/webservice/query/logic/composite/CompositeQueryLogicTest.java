package datawave.webservice.query.logic.composite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import datawave.marking.MarkingFunctions;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils.NpeUtils;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.cache.ResultsPage.Status;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.BaseQueryLogicTransformer;
import datawave.webservice.query.logic.DatawaveRoleManager;
import datawave.webservice.query.logic.EasyRoleManager;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.result.EdgeQueryResponseBase;
import datawave.webservice.query.result.edge.EdgeBase;
import datawave.webservice.result.BaseQueryResponse;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CompositeQueryLogicTest {
    
    private final Authorizations auths = new Authorizations("AUTHS");
    
    private Key key1 = new Key("one");
    private Key key2 = new Key("two");
    private Key key3 = new Key("three");
    private Key key4 = new Key("four");
    private Key key5 = new Key("five");
    private Key key6 = new Key("six");
    private Key key7 = new Key("seven");
    private Key key8 = new Key("eight");
    
    private Value value1 = new Value(key1.getRowData().getBackingArray());
    private Value value2 = new Value(key2.getRowData().getBackingArray());
    private Value value3 = new Value(key3.getRowData().getBackingArray());
    private Value value4 = new Value(key4.getRowData().getBackingArray());
    private Value value5 = new Value(key5.getRowData().getBackingArray());
    private Value value6 = new Value(key6.getRowData().getBackingArray());
    private Value value7 = new Value(key7.getRowData().getBackingArray());
    private Value value8 = new Value(key8.getRowData().getBackingArray());
    
    public static class TestQueryConfiguration extends GenericQueryConfiguration {
        
    }
    
    public static class TestQueryResponse extends BaseQueryResponse {
        
        private static final long serialVersionUID = 1L;
        
        private String key;
        private String value;
        
        public String getKey() {
            return key;
        }
        
        public String getValue() {
            return value;
        }
        
        public void setKey(String key) {
            this.key = key;
        }
        
        public void setValue(String value) {
            this.value = value;
        }
        
    }
    
    public static class TestEdgeQueryResponse extends EdgeQueryResponseBase {
        private ArrayList<EdgeBase> edges = new ArrayList<>();
        private long totalResults = 0;
        
        @Override
        public void addEdge(EdgeBase edge) {
            edges.add(edge);
        }
        
        @Override
        public List<? extends EdgeBase> getEdges() {
            return Collections.unmodifiableList(edges);
        }
        
        @Override
        public void setTotalResults(long totalResults) {
            this.totalResults = totalResults;
        }
        
        @Override
        public long getTotalResults() {
            return totalResults;
        }
        
        @Override
        public void setMarkings(Map<String,String> markings) {
            this.markings = markings;
        }
        
        @Override
        public Map<String,String> getMarkings() {
            return Collections.unmodifiableMap(markings);
        }
    }
    
    public static class TestQueryResponseList extends BaseQueryResponse {
        
        private static final long serialVersionUID = 1L;
        
        private List<TestQueryResponse> responses = new ArrayList<>();
        
        public List<TestQueryResponse> getResponses() {
            return responses;
        }
        
        public void setResponses(List<TestQueryResponse> responses) {
            this.responses = responses;
        }
        
        public void addResponse(TestQueryResponse response) {
            this.responses.add(response);
        }
        
    }
    
    public static class TestQueryLogicTransformer extends BaseQueryLogicTransformer<Entry<?,?>,TestQueryResponse> {
        
        public TestQueryLogicTransformer(MarkingFunctions markingFunctions) {
            super(markingFunctions);
        }
        
        @Override
        public TestQueryResponse transform(Entry<?,?> input) {
            if (input instanceof Entry<?,?>) {
                @SuppressWarnings("unchecked")
                Entry<Key,org.apache.accumulo.core.data.Value> entry = (Entry<Key,org.apache.accumulo.core.data.Value>) input;
                TestQueryResponse r = new TestQueryResponse();
                r.setKey(entry.getKey().toString());
                r.setValue(new String(entry.getValue().toString()));
                return r;
            } else {
                throw new IllegalArgumentException("Invalid input type: " + input.getClass());
            }
        }
        
        @Override
        public BaseQueryResponse createResponse(List<Object> resultList) {
            TestQueryResponseList response = new TestQueryResponseList();
            for (Object o : resultList) {
                TestQueryResponse r = (TestQueryResponse) o;
                response.addResponse(r);
            }
            return response;
        }
        
    }
    
    public static class DifferentTestQueryLogicTransformer extends BaseQueryLogicTransformer<Entry<?,?>,TestQueryResponse> {
        
        public DifferentTestQueryLogicTransformer(MarkingFunctions markingFunctions) {
            super(markingFunctions);
        }
        
        @Override
        public TestQueryResponse transform(Entry<?,?> input) {
            if (input instanceof Entry<?,?>) {
                @SuppressWarnings("unchecked")
                Entry<Key,org.apache.accumulo.core.data.Value> entry = (Entry<Key,org.apache.accumulo.core.data.Value>) input;
                TestQueryResponse r = new TestQueryResponse();
                r.setKey(entry.getKey().toString());
                r.setValue(new String(entry.getValue().get()));
                return r;
            } else {
                throw new IllegalArgumentException("Invalid input type: " + input.getClass());
            }
        }
        
        @Override
        public BaseQueryResponse createResponse(List<Object> resultList) {
            return new TestEdgeQueryResponse();
        }
        
    }
    
    public static class TestQueryLogic extends BaseQueryLogic<Entry<Key,Value>> {
        
        private Map<Key,Value> data = new ConcurrentHashMap<>();
        
        @Override
        public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
            return new TestQueryConfiguration();
        }
        
        @Override
        public void setupQuery(GenericQueryConfiguration configuration) throws Exception {}
        
        @Override
        public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                        throws Exception {
            return "";
        }
        
        @Override
        public Priority getConnectionPriority() {
            return Priority.NORMAL;
        }
        
        @Override
        public Iterator<Entry<Key,Value>> iterator() {
            return data.entrySet().iterator();
        }
        
        @Override
        public QueryLogicTransformer getTransformer(Query settings) {
            return new TestQueryLogicTransformer(new MarkingFunctions.Default());
        }
        
        @Override
        public Object clone() throws CloneNotSupportedException {
            return null;
        }
        
        public Map<Key,Value> getData() {
            return data;
        }
        
        @Override
        public String getLogicName() {
            return UUID.randomUUID().toString();
        }
        
        @Override
        public Set<String> getOptionalQueryParameters() {
            return null;
        }
        
        @Override
        public String getTableName() {
            return "table1";
        }
        
        @Override
        public Set<String> getRequiredQueryParameters() {
            return null;
        }
        
        @Override
        public Set<String> getExampleQueries() {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
    public static class TestQueryLogic2 extends TestQueryLogic {
        private Map<Key,Value> data = new ConcurrentHashMap<>();
        
        public Map<Key,Value> getData() {
            return data;
        }
        
        @Override
        public Iterator<Entry<Key,Value>> iterator() {
            return data.entrySet().iterator();
        }
        
        @Override
        public String getLogicName() {
            return UUID.randomUUID().toString();
        }
        
        @Override
        public QueryLogicTransformer getTransformer(Query settings) {
            return new TestQueryLogicTransformer(new MarkingFunctions.Default());
        }
        
        @Override
        public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
            return new TestQueryConfiguration();
        }
    }
    
    public static class DifferentTestQueryLogic extends BaseQueryLogic<Entry<Key,Value>> {
        
        @Override
        public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
            return new TestQueryConfiguration();
        }
        
        @Override
        public void setupQuery(GenericQueryConfiguration configuration) throws Exception {}
        
        @Override
        public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                        throws Exception {
            return "";
        }
        
        @Override
        public Priority getConnectionPriority() {
            return Priority.NORMAL;
        }
        
        @Override
        public QueryLogicTransformer getTransformer(Query settings) {
            return new DifferentTestQueryLogicTransformer(new MarkingFunctions.Default());
        }
        
        @Override
        public Object clone() throws CloneNotSupportedException {
            return null;
        }
        
        @Override
        public Set<String> getOptionalQueryParameters() {
            return Collections.emptySet();
        }
        
        @Override
        public Set<String> getRequiredQueryParameters() {
            return Collections.emptySet();
        }
        
        @Override
        public Set<String> getExampleQueries() {
            // TODO Auto-generated method stub
            return Collections.emptySet();
        }
        
    }
    
    @Before
    public void setup() {
        System.setProperty(NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
    }
    
    @Test(expected = RuntimeException.class)
    public void testInitializeWithSameQueryLogicAndTableNames() throws Exception {
        
        List<BaseQueryLogic<?>> logics = new ArrayList<>();
        logics.add(new TestQueryLogic());
        logics.add(new TestQueryLogic());
        
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());
        
        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);
        
        c.initialize(null, settings, Collections.singleton(auths));
    }
    
    @Test
    public void testInitializeWithSameQueryLogicAndDifferentTableNames() throws Exception {
        
        List<BaseQueryLogic<?>> logics = new ArrayList<>();
        logics.add(new TestQueryLogic() {
            @Override
            public String getTableName() {
                return "table1";
            }
        });
        logics.add(new TestQueryLogic() {
            @Override
            public String getTableName() {
                return "table2";
            }
        });
        
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());
        
        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);
        
        c.initialize(null, settings, Collections.singleton(auths));
    }
    
    @Test
    public void testInitialize() throws Exception {
        
        List<BaseQueryLogic<?>> logics = new ArrayList<>();
        logics.add(new TestQueryLogic());
        logics.add(new TestQueryLogic2());
        
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());
        
        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);
        
        c.initialize(null, settings, Collections.singleton(auths));
    }
    
    @Test
    public void testInitializeWithDifferentResponseTypes() throws Exception {
        
        List<BaseQueryLogic<?>> logics = new ArrayList<>();
        logics.add(new TestQueryLogic());
        logics.add(new DifferentTestQueryLogic());
        
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());
        
        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);
        
        c.initialize(null, settings, Collections.singleton(auths));
    }
    
    @Test
    public void testCloseWithNoSetup() throws Exception {
        
        List<BaseQueryLogic<?>> logics = new ArrayList<>();
        logics.add(new TestQueryLogic());
        logics.add(new TestQueryLogic2());
        
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());
        
        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);
        
        c.initialize(null, settings, Collections.singleton(auths));
        c.close();
    }
    
    @Test
    // testQueryLogic with max.results.override not set
    public void testQueryLogic() throws Exception {
        Logger.getLogger(CompositeQueryLogic.class).setLevel(Level.TRACE);
        Logger.getLogger(CompositeQueryLogicResults.class).setLevel(Level.TRACE);
        Logger.getLogger(CompositeQueryLogicTransformer.class).setLevel(Level.TRACE);
        List<BaseQueryLogic<?>> logics = new ArrayList<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.add(logic1);
        logics.add(logic2);
        
        logic1.getData().put(key1, value1);
        logic1.getData().put(key2, value2);
        logic2.getData().put(key3, value3);
        logic2.getData().put(key4, value4);
        logic1.getData().put(key5, value5);
        logic1.getData().put(key6, value6);
        logic2.getData().put(key7, value7);
        logic2.getData().put(key8, value8);
        
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());
        
        CompositeQueryLogic c = new CompositeQueryLogic();
        // max.results.override is set to -1 when it is not passed in as it is an optional paramter
        logic1.setMaxResults(-1);
        logic2.setMaxResults(-1);
        /**
         * RunningQuery.setupConnection()
         */
        c.setQueryLogics(logics);
        c.initialize(null, settings, Collections.singleton(auths));
        c.setupQuery(null);
        TransformIterator iter = c.getTransformIterator(settings);
        
        /**
         * RunningQuery.next() - iterate over results coming from tablet server through the TransformIterator to turn them into the objects.
         */
        List<Object> results = new ArrayList<>();
        while (iter.hasNext()) {
            Object o = iter.next();
            if (null == o)
                break;
            Assert.assertTrue(o instanceof TestQueryResponse);
            results.add(o);
        }
        Assert.assertEquals(8, results.size());
        ResultsPage page = new ResultsPage(results, Status.COMPLETE);
        
        /**
         * QueryExecutorBean.next() - transform list of objects into JAXB response
         */
        TestQueryResponseList response = (TestQueryResponseList) c.getTransformer(settings).createResponse(page);
        Assert.assertEquals(8, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }
        
        c.close();
        
    }
    
    @Test
    // testQueryLogic with max.results.override is set
    public void testQueryLogicWithMaxResultsOverride() throws Exception {
        Logger.getLogger(CompositeQueryLogic.class).setLevel(Level.TRACE);
        Logger.getLogger(CompositeQueryLogicResults.class).setLevel(Level.TRACE);
        Logger.getLogger(CompositeQueryLogicTransformer.class).setLevel(Level.TRACE);
        List<BaseQueryLogic<?>> logics = new ArrayList<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.add(logic1);
        logics.add(logic2);
        
        logic1.getData().put(key1, value1);
        logic1.getData().put(key2, value2);
        logic2.getData().put(key3, value3);
        logic2.getData().put(key4, value4);
        logic1.getData().put(key5, value5);
        logic1.getData().put(key6, value6);
        logic2.getData().put(key7, value7);
        logic2.getData().put(key8, value8);
        
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());
        
        CompositeQueryLogic c = new CompositeQueryLogic();
        // max.results.override is set to -1 when it is not passed in as it is an optional parameter
        logic1.setMaxResults(0);
        logic2.setMaxResults(4);
        /**
         * RunningQuery.setupConnection()
         */
        c.setQueryLogics(logics);
        c.initialize(null, settings, Collections.singleton(auths));
        c.setupQuery(null);
        TransformIterator iter = c.getTransformIterator(settings);
        
        /**
         * RunningQuery.next() - iterate over results coming from tablet server through the TransformIterator to turn them into the objects.
         */
        List<Object> results = new ArrayList<>();
        while (iter.hasNext()) {
            Object o = iter.next();
            if (null == o)
                break;
            Assert.assertTrue(o instanceof TestQueryResponse);
            results.add(o);
        }
        Assert.assertEquals(4, results.size());
        ResultsPage page = new ResultsPage(results, Status.COMPLETE);
        
        /**
         * QueryExecutorBean.next() - transform list of objects into JAXB response
         */
        TestQueryResponseList response = (TestQueryResponseList) c.getTransformer(settings).createResponse(page);
        Assert.assertEquals(4, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }
        
        c.close();
        
    }
    
    @Test
    public void testQueryLogicNoDataLogic1() throws Exception {
        List<BaseQueryLogic<?>> logics = new ArrayList<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.add(logic1);
        logics.add(logic2);
        
        logic2.getData().put(key1, value1);
        logic2.getData().put(key2, value2);
        logic2.getData().put(key3, value3);
        logic2.getData().put(key4, value4);
        logic2.getData().put(key5, value5);
        logic2.getData().put(key6, value6);
        logic2.getData().put(key7, value7);
        logic2.getData().put(key8, value8);
        
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());
        
        CompositeQueryLogic c = new CompositeQueryLogic();
        // max.results.override is set to -1 when it is not passed in as it is an optional paramter
        logic2.setMaxResults(-1);
        logic1.setMaxResults(-1);
        /**
         * RunningQuery.setupConnection()
         */
        c.setQueryLogics(logics);
        c.initialize(null, settings, Collections.singleton(auths));
        c.setupQuery(null);
        TransformIterator iter = c.getTransformIterator(settings);
        
        /**
         * RunningQuery.next() - iterate over results coming from tablet server through the TransformIterator to turn them into the objects.
         */
        List<Object> results = new ArrayList<>();
        while (iter.hasNext()) {
            Object o = iter.next();
            if (null == o)
                break;
            Assert.assertTrue(o instanceof TestQueryResponse);
            results.add(o);
        }
        Assert.assertEquals(8, results.size());
        ResultsPage page = new ResultsPage(results, Status.COMPLETE);
        
        /**
         * QueryExecutorBean.next() - transform list of objects into JAXB response
         */
        TestQueryResponseList response = (TestQueryResponseList) c.getTransformer(settings).createResponse(page);
        Assert.assertEquals(8, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }
        
        c.close();
        
    }
    
    @Test
    public void testQueryLogicNoDataLogic2() throws Exception {
        List<BaseQueryLogic<?>> logics = new ArrayList<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.add(logic1);
        logics.add(logic2);
        
        logic1.getData().put(key1, value1);
        logic1.getData().put(key2, value2);
        logic1.getData().put(key3, value3);
        logic1.getData().put(key4, value4);
        logic1.getData().put(key5, value5);
        logic1.getData().put(key6, value6);
        logic1.getData().put(key7, value7);
        logic1.getData().put(key8, value8);
        
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());
        
        CompositeQueryLogic c = new CompositeQueryLogic();
        // max.results.override is set to -1 when it is not passed in as it is an optional paramter
        logic2.setMaxResults(-1);
        logic1.setMaxResults(-1);
        /**
         * RunningQuery.setupConnection()
         */
        c.setQueryLogics(logics);
        c.initialize(null, settings, Collections.singleton(auths));
        c.setupQuery(null);
        TransformIterator iter = c.getTransformIterator(settings);
        
        /**
         * RunningQuery.next() - iterate over results coming from tablet server through the TransformIterator to turn them into the objects.
         */
        List<Object> results = new ArrayList<>();
        while (iter.hasNext()) {
            Object o = iter.next();
            if (null == o)
                break;
            Assert.assertTrue(o instanceof TestQueryResponse);
            results.add(o);
        }
        Assert.assertEquals(8, results.size());
        ResultsPage page = new ResultsPage(results, Status.COMPLETE);
        
        /**
         * QueryExecutorBean.next() - transform list of objects into JAXB response
         */
        TestQueryResponseList response = (TestQueryResponseList) c.getTransformer(settings).createResponse(page);
        Assert.assertEquals(8, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }
        
        c.close();
        
    }
    
    @Test
    public void testQueryLogicNoData() throws Exception {
        List<BaseQueryLogic<?>> logics = new ArrayList<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.add(logic1);
        logics.add(logic2);
        
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());
        
        CompositeQueryLogic c = new CompositeQueryLogic();
        /**
         * RunningQuery.setupConnection()
         */
        c.setQueryLogics(logics);
        c.initialize(null, settings, Collections.singleton(auths));
        c.setupQuery(null);
        TransformIterator iter = c.getTransformIterator(settings);
        
        /**
         * RunningQuery.next() - iterate over results coming from tablet server through the TransformIterator to turn them into the objects.
         */
        List<Object> results = new ArrayList<>();
        while (iter.hasNext()) {
            Object o = iter.next();
            if (null == o)
                break;
            Assert.assertTrue(o instanceof TestQueryResponse);
            results.add(o);
        }
        Assert.assertEquals(0, results.size());
        c.close();
        
    }
    
    @Test
    public void testCanRunQueryLogic() throws Exception {
        List<BaseQueryLogic<?>> logics = new ArrayList<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        HashSet<String> roles = new HashSet<>();
        roles.add("TESTROLE");
        logic1.setRoleManager(new DatawaveRoleManager(roles));
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logic2.setRoleManager(new EasyRoleManager());
        logics.add(logic1);
        logics.add(logic2);
        
        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);
        
        DatawaveUser u = new DatawaveUser(SubjectIssuerDNPair.of("CN=Other User Name ouser, OU=acme", "CN=ca, OU=acme"), UserType.USER, null,
                        Collections.singleton("TESTROLE"), null, 0L);
        DatawavePrincipal p = new DatawavePrincipal(Collections.singletonList(u));
        
        Assert.assertTrue(c.canRunQuery(p));
        Assert.assertEquals(2, c.getQueryLogics().size());
    }
    
    @Test
    public void testCanRunQueryLogic2() throws Exception {
        List<BaseQueryLogic<?>> logics = new ArrayList<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        HashSet<String> roles = new HashSet<>();
        roles.add("TESTROLE");
        logic1.setRoleManager(new DatawaveRoleManager(roles));
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        HashSet<String> roles2 = new HashSet<>();
        roles2.add("NONTESTROLE");
        logic2.setRoleManager(new DatawaveRoleManager(roles2));
        logics.add(logic1);
        logics.add(logic2);
        
        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);
        
        DatawaveUser u = new DatawaveUser(SubjectIssuerDNPair.of("CN=Other User Name ouser, OU=acme", "CN=ca, OU=acme"), UserType.USER, null,
                        Collections.singleton("TESTROLE"), null, 0L);
        DatawavePrincipal p = new DatawavePrincipal(Collections.singletonList(u));
        
        Assert.assertTrue(c.canRunQuery(p));
        Assert.assertEquals(1, c.getQueryLogics().size());
    }
    
    @Test
    public void testCannotRunQueryLogic2() throws Exception {
        List<BaseQueryLogic<?>> logics = new ArrayList<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        HashSet<String> roles = new HashSet<>();
        roles.add("NONTESTROLE");
        logic1.setRoleManager(new DatawaveRoleManager(roles));
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        HashSet<String> roles2 = new HashSet<>();
        roles2.add("NONTESTROLE");
        logic2.setRoleManager(new DatawaveRoleManager(roles2));
        logics.add(logic1);
        logics.add(logic2);
        
        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);
        
        DatawaveUser u = new DatawaveUser(SubjectIssuerDNPair.of("CN=Other User Name ouser, OU=acme", "CN=ca, OU=acme"), UserType.USER, null,
                        Collections.singleton("TESTROLE"), null, 0L);
        DatawavePrincipal p = new DatawavePrincipal(Collections.singletonList(u));
        
        Assert.assertFalse(c.canRunQuery(p));
        Assert.assertEquals(0, c.getQueryLogics().size());
        
    }
}
