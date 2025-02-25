package datawave.webservice.query.logic.composite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;

import datawave.core.common.connection.AccumuloConnectionFactory.Priority;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.cache.ResultsPage.Status;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.exception.EmptyObjectException;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.BaseQueryLogicTransformer;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.core.query.logic.composite.CompositeLogicException;
import datawave.core.query.logic.composite.CompositeQueryLogic;
import datawave.core.query.logic.filtered.FilteredQueryLogic;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.UserOperations;
import datawave.security.util.DnUtils;
import datawave.user.AuthorizationsListBase;
import datawave.user.DefaultAuthorizationsList;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.EdgeQueryResponseBase;
import datawave.webservice.query.result.edge.EdgeBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.QueryValidationResponse;

public class CompositeQueryLogicTest {

    private final Authorizations auths = new Authorizations("auth1", "auth2");

    private final DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("CN=Other User Name ouser, OU=acme", "CN=ca, OU=acme"), UserType.USER,
                    Arrays.asList("auth1", "auth2"), Collections.singleton("TESTROLE"), HashMultimap.create(), 0L);
    private final DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));

    private Key key1 = new Key("one", "", "", "auth1");
    private Key key2 = new Key("two", "", "", "auth1");
    private Key key3 = new Key("three", "", "", "auth2");
    private Key key4 = new Key("four", "", "", "auth2");
    private Key key5 = new Key("five", "", "", "auth2");
    private Key key6 = new Key("six", "", "", "auth2");
    private Key key7 = new Key("seven", "", "", "auth2");
    private Key key8 = new Key("eight", "", "", "auth1");
    private static final Key keyFailure = new Key("failure", "", "", "auth1");
    private static final Key keySpecial = new Key("special", "", "", "special");

    private Value value1 = new Value(key1.getRowData().getBackingArray());
    private Value value2 = new Value(key2.getRowData().getBackingArray());
    private Value value3 = new Value(key3.getRowData().getBackingArray());
    private Value value4 = new Value(key4.getRowData().getBackingArray());
    private Value value5 = new Value(key5.getRowData().getBackingArray());
    private Value value6 = new Value(key6.getRowData().getBackingArray());
    private Value value7 = new Value(key7.getRowData().getBackingArray());
    private Value value8 = new Value(key8.getRowData().getBackingArray());
    private Value valueFailure = new Value(keyFailure.getRowData().getBackingArray());
    private Value valueSpecial = new Value(keySpecial.getRowData().getBackingArray());

    private static final String VALIDATION_MESSAGE = "Light is green, the trap is clean";

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
        public void setEdges(List<EdgeBase> edges) {
            this.edges = new ArrayList<>(edges);
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
        public TestQueryResponse transform(Entry<?,?> input) throws EmptyObjectException {
            if (input instanceof Entry<?,?>) {
                @SuppressWarnings("unchecked")
                Entry<Key,org.apache.accumulo.core.data.Value> entry = (Entry<Key,org.apache.accumulo.core.data.Value>) input;
                if (entry.getValue() == null) {
                    throw new EmptyObjectException();
                }
                // first check if we should be failing here
                if (entry.getKey().equals(keyFailure)) {
                    throw new RuntimeException(entry.getValue().toString());
                }
                TestQueryResponse r = new TestQueryResponse();
                r.setKey(entry.getKey().toString());
                r.setValue(entry.getValue().toString());
                r.setHasResults(true);
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
        public TestQueryResponse transform(Entry<?,?> input) throws EmptyObjectException {
            if (input instanceof Entry<?,?>) {
                @SuppressWarnings("unchecked")
                Entry<Key,org.apache.accumulo.core.data.Value> entry = (Entry<Key,org.apache.accumulo.core.data.Value>) input;
                if (entry.getValue() == null) {
                    throw new EmptyObjectException();
                }
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

        private Map<Key,Value> data = Collections.synchronizedMap(new LinkedHashMap<>());

        private final UserOperations userOperations;
        private Set<Authorizations> auths;

        public TestQueryLogic() {
            this(null);
        }

        public TestQueryLogic(UserOperations userOperations) {
            this.userOperations = userOperations;
        }

        @Override
        public UserOperations getUserOperations() {
            return userOperations;
        }

        @Override
        public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
            this.auths = runtimeQueryAuthorizations;
            TestQueryConfiguration config = new TestQueryConfiguration();
            config.setAuthorizations(runtimeQueryAuthorizations);
            return config;
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
            return data.entrySet().stream().filter(e -> checkAuths(e.getKey().getColumnVisibilityParsed())).iterator();
        }

        private boolean checkAuths(ColumnVisibility vis) {
            return auths.stream().allMatch(a -> {
                try {
                    return new VisibilityEvaluator(a).evaluate(vis);
                } catch (VisibilityParseException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public QueryLogicTransformer getTransformer(Query settings) {
            return new TestQueryLogicTransformer(new MarkingFunctions.Default());
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return new TestQueryLogic();
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
            return null;
        }

    }

    public static class TestUserOperations implements UserOperations {

        @Override
        public AuthorizationsListBase listEffectiveAuthorizations(ProxiedUserDetails callerObject) throws AuthorizationException {
            DatawavePrincipal p = (DatawavePrincipal) callerObject;
            DefaultAuthorizationsList authList = new DefaultAuthorizationsList();
            DatawaveUser primaryUser = p.getPrimaryUser();
            Set<String> auths = Collections.singleton("special");
            authList.setUserAuths(primaryUser.getDn().subjectDN(), p.getPrimaryUser().getDn().issuerDN(), auths);
            for (DatawaveUser u : p.getProxiedUsers()) {
                if (u != primaryUser) {
                    authList.addAuths(u.getDn().subjectDN(), u.getDn().issuerDN(), auths);
                }
            }
            return authList;
        }

        @Override
        public GenericResponse<String> flushCachedCredentials(ProxiedUserDetails callerObject) {
            return new GenericResponse<>();
        }
    }

    public static class TestQueryLogic2 extends TestQueryLogic {

        private Map<Key,Value> data = Collections.synchronizedMap(new LinkedHashMap<>());

        public Map<Key,Value> getData() {
            return data;
        }

        @Override
        public Iterator<Entry<Key,Value>> iterator() {
            return data.entrySet().iterator();
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return new TestQueryLogic2();
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

        @Override
        public boolean isLongRunningQuery() {
            return true;
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
            return new DifferentTestQueryLogic();
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

        @Override
        public Object validateQuery(AccumuloClient client, Query query, Set<Authorizations> auths) throws Exception {
            // return something valid
            return VALIDATION_MESSAGE;
        }

        @Override
        public Transformer<Object,QueryValidationResponse> getQueryValidationResponseTransformer() {
            return new TestQueryValidationResultTransformer();
        }

    }

    public static class TestQueryValidationResultTransformer implements Transformer<Object,QueryValidationResponse> {

        @Override
        public QueryValidationResponse transform(Object input) {
            String validation = String.valueOf(input);
            QueryValidationResponse.Result result = new QueryValidationResponse.Result();
            result.setMessages(Collections.singletonList(validation));

            QueryValidationResponse response = new QueryValidationResponse();
            response.setHasResults(true);
            response.setResults(Collections.singletonList(result));

            return response;
        }
    }

    public static class TestFilteredQueryLogic extends FilteredQueryLogic {
        private boolean filtered;

        public TestFilteredQueryLogic(boolean filtered) {
            QueryLogic delegate = new TestQueryLogic();
            setDelegate(delegate);
            this.filtered = filtered;
        }

        @Override
        public boolean isFiltered() {
            return filtered;
        }
    }

    @Before
    public void setup() {
        System.setProperty(DnUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
    }

    @Test
    public void testClone() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        logics.put("TestQueryLogic", new TestQueryLogic());
        logics.put("TestQueryLogic2", new TestQueryLogic());

        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);
        c = (CompositeQueryLogic) c.clone();

        c.setCurrentUser(principal);
        c.initialize(null, settings, Collections.singleton(auths));
        c.getTransformer(settings);

        Assert.assertEquals(2, c.getInitializedLogics().size());
    }

    @Test
    public void testInitializeOKWithSameQueryLogicAndTableNames() throws Exception {

        Map<String,QueryLogic<?>> logics = new HashMap<>();
        logics.put("TestQueryLogic", new TestQueryLogic());
        logics.put("TestQueryLogic2", new TestQueryLogic());

        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);

        c.setCurrentUser(principal);
        c.initialize(null, settings, Collections.singleton(auths));
        c.getTransformer(settings);

        Assert.assertEquals(2, c.getInitializedLogics().size());
    }

    @Test
    public void testInitializeWithSameQueryLogicAndDifferentTableNames() throws Exception {

        Map<String,QueryLogic<?>> logics = new HashMap<>();
        logics.put("TestQueryLogic", new TestQueryLogic() {
            @Override
            public String getTableName() {
                return "table1";
            }
        });
        logics.put("TestQueryLogic2", new TestQueryLogic() {
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

        c.setCurrentUser(principal);
        c.initialize(null, settings, Collections.singleton(auths));

        c.getTransformer(settings);

        Assert.assertEquals(2, c.getInitializedLogics().size());
    }

    @Test
    public void testInitialize() throws Exception {

        Map<String,QueryLogic<?>> logics = new HashMap<>();
        logics.put("TestQueryLogic", new TestQueryLogic());
        logics.put("TestQueryLogic2", new TestQueryLogic2());

        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);

        c.setCurrentUser(principal);
        c.initialize(null, settings, Collections.singleton(auths));

        c.getTransformer(settings);

        Assert.assertEquals(2, c.getInitializedLogics().size());
    }

    @Test
    public void testInitializeOKWithFailure() throws Exception {

        Map<String,QueryLogic<?>> logics = new HashMap<>();
        logics.put("TestQueryLogic", new TestQueryLogic());
        logics.put("TestQueryLogic2", new TestQueryLogic2() {
            @Override
            public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations)
                            throws Exception {
                throw new Exception("initialize failed");
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

        c.setCurrentUser(principal);
        c.initialize(null, settings, Collections.singleton(auths));

        Assert.assertEquals(1, c.getInitializedLogics().size());
    }

    @Test
    public void testInitializeOKWithFilter() throws Exception {

        Map<String,QueryLogic<?>> logics = new HashMap<>();
        logics.put("TestQueryLogic", new TestQueryLogic());
        logics.put("TestQueryLogic2", new TestFilteredQueryLogic(true));

        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);

        c.setCurrentUser(principal);
        c.initialize(null, settings, Collections.singleton(auths));

        Assert.assertEquals(1, c.getInitializedLogics().size());
        // ensure the filtered query logic is actually dropped
        Assert.assertEquals(0, c.getUninitializedLogics().size());
    }

    @Test(expected = CompositeLogicException.class)
    public void testInitializeNotOKWithFilter() throws Exception {

        Map<String,QueryLogic<?>> logics = new HashMap<>();
        logics.put("TestQueryLogic", new TestQueryLogic() {
            @Override
            public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations)
                            throws Exception {
                throw new Exception("initialize failed");
            }
        });
        logics.put("TestQueryLogic2", new TestFilteredQueryLogic(true));

        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);

        // testing that we fail despite allMustInitialize to false because the filtered logic does not count
        c.setAllMustInitialize(false);

        c.setCurrentUser(principal);
        c.initialize(null, settings, Collections.singleton(auths));
    }

    @Test(expected = CompositeLogicException.class)
    public void testInitializeNotOKWithFailure() throws Exception {

        Map<String,QueryLogic<?>> logics = new HashMap<>();
        logics.put("TestQueryLogic", new TestQueryLogic());
        logics.put("TestQueryLogic2", new TestQueryLogic2() {
            @Override
            public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations)
                            throws Exception {
                throw new Exception("initialize failed");
            }
        });

        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setAllMustInitialize(true);
        c.setQueryLogics(logics);

        c.setCurrentUser(principal);
        c.initialize(null, settings, Collections.singleton(auths));
    }

    @Test(expected = CompositeLogicException.class)
    public void testInitializeAllFail() throws Exception {

        Map<String,QueryLogic<?>> logics = new HashMap<>();
        logics.put("TestQueryLogic", new TestQueryLogic() {
            @Override
            public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations)
                            throws Exception {
                throw new Exception("initialize failed");
            }
        });

        logics.put("TestQueryLogic2", new TestQueryLogic2() {
            @Override
            public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations)
                            throws Exception {
                throw new Exception("initialize failed");
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

        c.setCurrentUser(principal);
        c.initialize(null, settings, Collections.singleton(auths));
    }

    @Test(expected = CompositeLogicException.class)
    public void testInitializeAllFail2() throws Exception {

        Map<String,QueryLogic<?>> logics = new HashMap<>();
        logics.put("TestQueryLogic", new TestQueryLogic() {
            @Override
            public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations)
                            throws Exception {
                throw new Exception("initialize failed");
            }
        });

        logics.put("TestQueryLogic2", new TestQueryLogic2() {
            @Override
            public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations)
                            throws Exception {
                throw new Exception("initialize failed");
            }
        });

        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setAllMustInitialize(true);
        c.setQueryLogics(logics);

        c.setCurrentUser(principal);
        c.initialize(null, settings, Collections.singleton(auths));

        c.getTransformer(settings);
    }

    @Test
    public void testInitializeAllFailQueryExceptionCause() throws Exception {

        Map<String,QueryLogic<?>> logics = new HashMap<>();
        logics.put("TestQueryLogic", new TestQueryLogic() {
            @Override
            public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations)
                            throws Exception {
                throw new Exception("initialize failed");
            }
        });

        logics.put("TestQueryLogic2", new TestQueryLogic2() {
            @Override
            public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations)
                            throws Exception {
                throw new RuntimeException(new QueryException("query initialize failed"));
            }
        });

        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setAllMustInitialize(true);
        c.setQueryLogics(logics);

        c.setCurrentUser(principal);
        try {
            c.initialize(null, settings, Collections.singleton(auths));

            c.getTransformer(settings);
        } catch (CompositeLogicException e) {
            Assert.assertEquals("datawave.webservice.query.exception.QueryException: query initialize failed", e.getCause().getCause().getMessage());
        }
    }

    @Test(expected = RuntimeException.class)
    public void testInitializeWithDifferentResponseTypes() throws Exception {

        Map<String,QueryLogic<?>> logics = new HashMap<>();
        logics.put("TestQueryLogic", new TestQueryLogic());
        logics.put("TestQueryLogic2", new DifferentTestQueryLogic());

        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);

        c.setCurrentUser(principal);
        c.initialize(null, settings, Collections.singleton(auths));

        c.getTransformer(settings);
    }

    @Test
    public void testCloseWithNoSetup() throws Exception {

        Map<String,QueryLogic<?>> logics = new HashMap<>();
        logics.put("TestQueryLogic", new TestQueryLogic());
        logics.put("TestQueryLogic2", new TestQueryLogic2());

        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);

        c.setCurrentUser(principal);
        c.initialize(null, settings, Collections.singleton(auths));

        c.getTransformer(settings);

        c.close();
    }

    @Test
    public void testQueryLogic() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

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
        c.setCurrentUser(principal);
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
        TestQueryResponseList response = (TestQueryResponseList) c.getEnrichedTransformer((Query) settings).createResponse(page);
        Assert.assertEquals(8, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }

        c.close();

    }

    @Test
    public void testQueryLogicWithEmptyEvent() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

        logic1.getData().put(key1, value1);
        logic1.getData().put(key2, null);
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
        c.setCurrentUser(principal);
        c.initialize((AccumuloClient) null, (Query) settings, Collections.singleton(auths));
        c.setupQuery(null);
        TransformIterator iter = c.getTransformIterator((Query) settings);

        /**
         * RunningQuery.next() - iterate over results coming from tablet server through the TransformIterator to turn them into the objects.
         */
        List<Object> results = new ArrayList<>();
        while (iter.hasNext()) {
            Object o = iter.next();
            if (null == o)
                break;
            Assert.assertTrue(o instanceof TestQueryResponse);
            results.add((TestQueryResponse) o);
        }
        Assert.assertEquals(7, results.size());
        ResultsPage page = new ResultsPage(results, Status.COMPLETE);

        /**
         * QueryExecutorBean.next() - transform list of objects into JAXB response
         */
        TestQueryResponseList response = (TestQueryResponseList) c.getEnrichedTransformer((Query) settings).createResponse(page);
        Assert.assertEquals(7, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }

        c.close();

    }

    @Test
    public void testQueryLogicShortCircuitExecution() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

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
        c.setCurrentUser(principal);
        c.setShortCircuitExecution(true);
        c.initialize((AccumuloClient) null, (Query) settings, Collections.singleton(auths));
        c.setupQuery(null);
        TransformIterator iter = c.getTransformIterator((Query) settings);

        /**
         * RunningQuery.next() - iterate over results coming from tablet server through the TransformIterator to turn them into the objects.
         */
        List<Object> results = new ArrayList<>();
        while (iter.hasNext()) {
            Object o = iter.next();
            if (null == o)
                break;
            Assert.assertTrue(o instanceof TestQueryResponse);
            results.add((TestQueryResponse) o);
        }
        // only half the results if both had been run
        Assert.assertEquals(4, results.size());
        ResultsPage page = new ResultsPage(results, Status.COMPLETE);
        Assert.assertFalse(c.getUninitializedLogics().isEmpty());
        Assert.assertFalse(c.getInitializedLogics().isEmpty());

        /**
         * QueryExecutorBean.next() - transform list of objects into JAXB response
         */
        TestQueryResponseList response = (TestQueryResponseList) c.getEnrichedTransformer((Query) settings).createResponse(page);
        Assert.assertEquals(4, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }

        c.close();

    }

    @Test
    public void testQueryLogicShortCircuitExecutionWithEmptyEvent() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

        logic1.getData().put(key1, value1);
        logic1.getData().put(key2, null);
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
        c.setCurrentUser(principal);
        c.setShortCircuitExecution(true);
        c.initialize((AccumuloClient) null, (Query) settings, Collections.singleton(auths));
        c.setupQuery(null);
        TransformIterator iter = c.getTransformIterator((Query) settings);

        /**
         * RunningQuery.next() - iterate over results coming from tablet server through the TransformIterator to turn them into the objects.
         */
        List<Object> results = new ArrayList<>();
        while (iter.hasNext()) {
            Object o = iter.next();
            if (null == o)
                break;
            Assert.assertTrue(o instanceof TestQueryResponse);
            results.add((TestQueryResponse) o);
        }
        // only half the results if both had been run
        Assert.assertEquals(3, results.size());
        ResultsPage page = new ResultsPage(results, Status.COMPLETE);
        Assert.assertFalse(c.getUninitializedLogics().isEmpty());
        Assert.assertFalse(c.getInitializedLogics().isEmpty());

        /**
         * QueryExecutorBean.next() - transform list of objects into JAXB response
         */
        TestQueryResponseList response = (TestQueryResponseList) c.getEnrichedTransformer((Query) settings).createResponse(page);
        Assert.assertEquals(3, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }

        c.close();

    }

    @Test
    public void testQueryLogicShortCircuitExecutionHitsSecondLogic() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

        logic1.getData().put(key1, null);
        logic2.getData().put(key3, value3);
        logic2.getData().put(key4, value4);
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
        c.setCurrentUser(principal);
        c.setShortCircuitExecution(true);
        c.initialize((AccumuloClient) null, (Query) settings, Collections.singleton(auths));
        c.setupQuery(null);
        TransformIterator iter = c.getTransformIterator((Query) settings);

        /**
         * RunningQuery.next() - iterate over results coming from tablet server through the TransformIterator to turn them into the objects.
         */
        List<Object> results = new ArrayList<>();
        while (iter.hasNext()) {
            Object o = iter.next();
            if (null == o)
                break;
            Assert.assertTrue(o instanceof TestQueryResponse);
            results.add((TestQueryResponse) o);
        }
        Assert.assertEquals(4, results.size());
        ResultsPage page = new ResultsPage(results, Status.COMPLETE);

        // ensure both were actually run
        Assert.assertTrue(c.getUninitializedLogics().isEmpty());
        Assert.assertFalse(c.getInitializedLogics().isEmpty());

        /**
         * QueryExecutorBean.next() - transform list of objects into JAXB response
         */
        TestQueryResponseList response = (TestQueryResponseList) c.getEnrichedTransformer((Query) settings).createResponse(page);
        Assert.assertEquals(4, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }

        c.close();

    }

    @Test(expected = CompositeLogicException.class)
    public void testQueryLogicWithNextFailure() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

        logic1.getData().put(key1, value1);
        logic1.getData().put(key2, value2);
        logic2.getData().put(key3, value3);
        logic2.getData().put(key4, value4);
        logic1.getData().put(key5, value5);
        logic1.getData().put(key6, value6);
        logic2.getData().put(keyFailure, new Value("Failure forced here"));
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
        c.setCurrentUser(principal);
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
            results.add((TestQueryResponse) o);
        }
    }

    @Test
    // testQueryLogic with max.results.override is set
    public void testQueryLogicWithMaxResultsOverride() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

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
        logic1.setMaxResults(2); // it can return 4, so this will cap it at 3 (1 more than max)
        logic2.setMaxResults(1); // it cat return 4, so this will cap it at 2 (1 more than max)
        /**
         * RunningQuery.setupConnection()
         */
        c.setQueryLogics(logics);
        c.setCurrentUser(principal);
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
        Assert.assertEquals(5, results.size());
        ResultsPage page = new ResultsPage(results, Status.COMPLETE);

        /**
         * QueryExecutorBean.next() - transform list of objects into JAXB response
         */
        TestQueryResponseList response = (TestQueryResponseList) c.getEnrichedTransformer((Query) settings).createResponse(page);
        Assert.assertEquals(5, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }

        c.close();

    }

    @Test
    // testQueryLogic with max.results.override is set
    public void testQueryLogicWithMaxResultsOverrideWithDNOverride() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

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
        settings.setDnList(Arrays.asList(principal.getUserDN().subjectDN()));

        CompositeQueryLogic c = new CompositeQueryLogic();
        // max.results.override is set to -1 when it is not passed in as it is an optional parameter
        logic1.setMaxResults(2); // it can return 4, so this will cap it at 3 (1 more than max)
        logic2.setMaxResults(1); // it cat return 4, so this will cap it at 2 (1 more than max)

        // just FYI, setting up DNResultLimits for the composite query logic doesn't do anything
        // c.setDnResultLimits(Map.of(principal.getUserDN().subjectDN(), 3L));

        // settings the DNResultLimits for each logics configured for composite
        logic1.setDnResultLimits(Map.of(principal.getUserDN().subjectDN(), 2L));
        logic2.setDnResultLimits(Map.of(principal.getUserDN().subjectDN(), 3L));
        /**
         * RunningQuery.setupConnection()
         */
        c.setQueryLogics(logics);
        c.setCurrentUser(principal);
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
        Assert.assertEquals(7, results.size());
        ResultsPage page = new ResultsPage(results, Status.COMPLETE);

        /**
         * QueryExecutorBean.next() - transform list of objects into JAXB response
         */
        TestQueryResponseList response = (TestQueryResponseList) c.getEnrichedTransformer((Query) settings).createResponse(page);
        Assert.assertEquals(7, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }

        c.close();

    }

    @Test
    // testQueryLogic with max.results.override is set
    public void testQueryLogicWithMaxResultsOverrideWithDNOverrideNonMatchingDN() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

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
        settings.setDnList(Arrays.asList(principal.getUserDN().subjectDN()));

        CompositeQueryLogic c = new CompositeQueryLogic();
        // max.results.override is set to -1 when it is not passed in as it is an optional parameter
        logic1.setMaxResults(2); // it can return 4, so this will cap it at 3 (1 more than max)
        logic2.setMaxResults(1); // it cat return 4, so this will cap it at 2 (1 more than max)

        // setting up DNResultLimits for the composite query logic
        // c.setDnResultLimits(Map.of(principal.getUserDN().toString(), 3L));

        logic1.setDnResultLimits(Map.of(principal.getUserDN().subjectDN() + "foo", 2L));
        logic2.setDnResultLimits(Map.of(principal.getUserDN().subjectDN() + "bar", 3L));
        /**
         * RunningQuery.setupConnection()
         */
        c.setQueryLogics(logics);
        c.setCurrentUser(principal);
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
        Assert.assertEquals(5, results.size());
        ResultsPage page = new ResultsPage(results, Status.COMPLETE);

        /**
         * QueryExecutorBean.next() - transform list of objects into JAXB response
         */
        TestQueryResponseList response = (TestQueryResponseList) c.getEnrichedTransformer((Query) settings).createResponse(page);
        Assert.assertEquals(5, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }

        c.close();

    }

    @Test
    public void testQueryLogicNoDataLogic1() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

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
        c.setCurrentUser(principal);
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
        TestQueryResponseList response = (TestQueryResponseList) c.getEnrichedTransformer((Query) settings).createResponse(page);
        Assert.assertEquals(8, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }

        c.close();

    }

    @Test
    public void testQueryLogicNoDataLogic2() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

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
        c.setCurrentUser(principal);
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
        TestQueryResponseList response = (TestQueryResponseList) c.getEnrichedTransformer((Query) settings).createResponse(page);
        Assert.assertEquals(8, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }

        c.close();

    }

    @Test
    public void testQueryLogicNoData() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

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
        c.setCurrentUser(principal);
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
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        HashSet<String> roles = new HashSet<>();
        roles.add("TESTROLE");
        logic1.setRequiredRoles(roles);
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logic2.setRequiredRoles(Collections.emptySet());
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);

        Assert.assertTrue(c.canRunQuery(Collections.singleton("TESTROLE")));
        Assert.assertEquals(2, c.getQueryLogics().size());
    }

    @Test
    public void testCanRunQueryLogic2() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        HashSet<String> roles = new HashSet<>();
        roles.add("TESTROLE");
        logic1.setRequiredRoles(roles);
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        HashSet<String> roles2 = new HashSet<>();
        roles2.add("NONTESTROLE");
        logic2.setRequiredRoles(roles2);
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);

        Assert.assertTrue(c.canRunQuery(Collections.singleton("TESTROLE")));
        Assert.assertEquals(1, c.getQueryLogics().size());
    }

    @Test
    public void testCannotRunQueryLogic2() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        HashSet<String> roles = new HashSet<>();
        roles.add("NONTESTROLE");
        logic1.setRequiredRoles(roles);
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        HashSet<String> roles2 = new HashSet<>();
        roles2.add("NONTESTROLE");
        logic2.setRequiredRoles(roles2);
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);

        Assert.assertFalse(c.canRunQuery(Collections.singleton("TESTROLE")));
        Assert.assertEquals(0, c.getQueryLogics().size());

    }

    @Test
    public void testIsLongRunningQuery() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic logic2 = new TestQueryLogic();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);

        Assert.assertFalse(c.isLongRunningQuery());

        TestQueryLogic2 logic3 = new TestQueryLogic2();
        logics.put("TestQueryLogic3", logic3);

        c.setQueryLogics(logics);

        Assert.assertTrue(c.isLongRunningQuery());
    }

    @Test
    public void testAuthorizationsUpdate() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic(new TestUserOperations());
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

        // logic 1 user opts only has the special auth returned by the TestUserOperations()
        logic1.getData().put(keySpecial, valueSpecial);
        // hence these 2 keys will not be returned
        logic1.getData().put(key1, value1);
        logic1.getData().put(key2, value2);

        // logic 1 should return all of these
        logic2.getData().put(key3, value3);
        logic2.getData().put(key4, value4);
        logic2.getData().put(key5, value5);
        logic2.getData().put(key6, value6);
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
        c.setCurrentUser(principal);
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
            results.add((TestQueryResponse) o);
        }
        Assert.assertEquals(7, results.size());
        ResultsPage page = new ResultsPage(results, Status.COMPLETE);

        /**
         * QueryExecutorBean.next() - transform list of objects into JAXB response
         */
        TestQueryResponseList response = (TestQueryResponseList) c.getEnrichedTransformer((Query) settings).createResponse(page);
        Assert.assertEquals(7, response.getResponses().size());
        for (TestQueryResponse r : response.getResponses()) {
            Assert.assertNotNull(r);
        }

        c.close();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValidationFails() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        TestQueryLogic2 logic2 = new TestQueryLogic2();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);
        c.setCurrentUser(principal);

        c.validateQuery(null, settings, Collections.singleton(auths));
    }

    @Test
    public void testValidationHappyPath() throws Exception {
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic logic1 = new TestQueryLogic();
        DifferentTestQueryLogic logic2 = new DifferentTestQueryLogic();
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);

        QueryImpl settings = new QueryImpl();
        settings.setPagesize(100);
        settings.setQueryAuthorizations(auths.toString());
        settings.setQuery("FOO == 'BAR'");
        settings.setParameters(new HashSet<>());
        settings.setId(UUID.randomUUID());

        CompositeQueryLogic c = new CompositeQueryLogic();
        c.setQueryLogics(logics);
        c.setCurrentUser(principal);

        Object validation = c.validateQuery(null, settings, Collections.singleton(auths));
        Transformer<Object,QueryValidationResponse> transformer = c.getQueryValidationResponseTransformer();
        QueryValidationResponse response = transformer.transform(validation);
        Assert.assertEquals(VALIDATION_MESSAGE, response.getResults().get(0).getMessages().get(0));
    }
}
