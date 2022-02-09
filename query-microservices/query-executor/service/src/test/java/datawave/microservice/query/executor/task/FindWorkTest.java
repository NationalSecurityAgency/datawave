package datawave.microservice.query.executor.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.Result;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.Constants;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import datawave.security.util.DnUtils;
import datawave.services.common.connection.AccumuloConnectionFactory;
import datawave.services.common.result.ConnectionPool;
import datawave.services.query.logic.QueryLogicFactory;
import datawave.services.query.predict.QueryPredictor;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.JEXL_AND_OP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"QueryStarterDefaults", "QueryStarterOverrides", "FindWorkTest", "use-test"})
@ContextConfiguration(classes = FindWorkTest.FindWorkTestConfiguration.class)
public class FindWorkTest {
    private static final Logger log = Logger.getLogger(FindWorkTest.class);
    
    private static DataTypeHadoopConfig dataType;
    private static List<TestAccumuloSetup> dataToCleanup = new ArrayList<>();
    
    @Autowired
    public TestAccumuloSetup accumuloSetup;
    
    @Autowired
    private QueryLogicFactory queryLogicFactory;
    
    @Autowired
    private ExecutorProperties executorProperties;
    
    @Autowired
    private QueryProperties queryProperties;
    
    @Autowired
    private BusProperties busProperties;
    
    @Autowired
    private ApplicationContext appCtx;
    
    @Autowired
    private ApplicationEventPublisher publisher;
    
    @Autowired
    protected QueryStorageCache storageService;
    
    @Autowired
    private QueryResultsManager queueManager;
    
    @Autowired
    private LinkedList<RemoteQueryRequestEvent> queryRequestsEvents;
    
    @Autowired
    protected AccumuloConnectionFactory connectionFactory;
    
    @Autowired
    protected List<QueryMetricClient.Request> requests;
    
    @Autowired
    protected QueryMetricClient metricClient;
    
    @Autowired
    protected QueryMetricFactory metricFactory;
    
    @Autowired
    protected Queue<QueryRequest> queryRequests;
    
    @Autowired
    protected FindWorkTask findWorkTask;
    
    public String TEST_POOL = "TestPool";
    
    public static HashSet<BaseQueryMetric.Prediction> predictions = Sets.newHashSet(new BaseQueryMetric.Prediction("Success", 0.5d),
                    new BaseQueryMetric.Prediction("Elephants", 0.0d));
    
    static {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        System.setProperty("file.encoding", StandardCharsets.UTF_8.name());
        System.setProperty(DnUtils.NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        try {
            File dir = new File(ClassLoader.getSystemClassLoader().getResource(".").toURI());
            File targetDir = dir.getParentFile();
            System.setProperty("DATAWAVE_INGEST_HOME", targetDir.getAbsolutePath());
            System.setProperty("hadoop.home.dir", targetDir.getAbsolutePath());
        } catch (URISyntaxException se) {
            log.error("failed to get URI for .", se);
            Assert.fail();
        }
        FieldConfig generic = new GenericCityFields();
        generic.addReverseIndexField(CitiesDataType.CityField.STATE.name());
        generic.addReverseIndexField(CitiesDataType.CityField.CONTINENT.name());
        try {
            dataType = new CitiesDataType(CitiesDataType.CityEntry.generic, generic);
        } catch (Exception e) {
            log.error("Failed to load cities data type", e);
            Assert.fail();
        }
    }
    
    @AfterAll
    public static void cleanupData() {
        synchronized (dataToCleanup) {
            for (TestAccumuloSetup setup : dataToCleanup) {
                setup.after();
            }
            dataToCleanup.clear();
        }
    }
    
    @Test
    public void testFindWorkQuery() throws Exception {
        // ensure the query requests starts as empty
        assertTrue(queryRequests.isEmpty());
        
        String city = "rome";
        String country = "italy";
        String queryStr = CitiesDataType.CityField.CITY.name() + ":\"" + city + "\"" + AND_OP + "#EVALUATION_ONLY('" + CitiesDataType.CityField.COUNTRY.name()
                        + ":\"" + country + "\"')";
        
        String expectPlan = CitiesDataType.CityField.CITY.name() + EQ_OP + "'" + city + "'" + JEXL_AND_OP + "((_Eval_ = true) && ("
                        + CitiesDataType.CityField.COUNTRY.name() + EQ_OP + "'" + country + "'))";
        
        Query query = new QueryImpl();
        query.setQuery(queryStr);
        query.setQueryLogicName("EventQuery");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20150101"));
        query.setEndDate(new SimpleDateFormat("yyyyMMdd").parse("20160101"));
        query.setQueryAuthorizations(CitiesDataType.getTestAuths().toString());
        query.setQueryName("TestQuery");
        query.setDnList(Collections.singletonList("test user"));
        query.setUserDN("test user");
        query.setPagesize(100);
        query.addParameter("query.syntax", "LUCENE");
        String queryPool = new String(TEST_POOL);
        TaskKey key = storageService.createQuery(queryPool, query, Collections.singleton(CitiesDataType.getTestAuths()), 20);
        assertNotNull(key);
        
        QueryStatus queryStatusTest = storageService.getQueryStatus(key.getQueryId());
        
        TaskStates states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.READY, states.getState(key.getTaskId()));
        
        // execute the find work task, and nothing should be found
        findWorkTask.call();
        
        // Should have create a create requeyst
        assertEquals(1, queryRequests.size());
        QueryRequest request = queryRequests.poll();
        assertEquals(key.getQueryId(), request.getQueryId());
        assertEquals(QueryRequest.Method.CREATE, request.getMethod());
        
        // now put the task in a running state
        storageService.updateTaskState(key, TaskStates.TASK_STATE.RUNNING);
        states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.RUNNING, states.getState(key.getTaskId()));
        
        // wait until the timeout
        Thread.sleep(executorProperties.getOrphanThresholdMs() + 500);
        
        // execute the find work task to reset the now orphaned task
        findWorkTask.call();
        
        // verify the task is now back in a ready state
        states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.READY, states.getState(key.getTaskId()));
        
    }
    
    private void checkFailed(String queryId) {
        QueryStatus status = storageService.getQueryStatus(queryId);
        if (status.getQueryState() == QueryStatus.QUERY_STATE.FAILED) {
            throw new RuntimeException(status.getFailureMessage() + " : " + status.getStackTrace());
        }
    }
    
    public static class TestAccumuloSetup extends AccumuloSetup {
        
        @Override
        public void after() {
            super.after();
        }
        
        @Override
        public void before() {
            try {
                super.before();
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }
    
    @Configuration
    @Profile("FindWorkTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class FindWorkTestConfiguration {
        @Bean
        @Primary
        public QueryExecutor executor(Queue<QueryRequest> queryRequests, ExecutorProperties executorProperties, QueryProperties queryProperties,
                        BusProperties busProperties, ApplicationContext appCtx, AccumuloConnectionFactory connectionFactory, QueryStorageCache cache,
                        QueryResultsManager queues, QueryLogicFactory queryLogicFactory, QueryPredictor predictor, ApplicationEventPublisher publisher,
                        QueryMetricFactory metricFactory, QueryMetricClient metricClient) {
            return new TestQueryExecutor(queryRequests, executorProperties, queryProperties, busProperties, appCtx, connectionFactory, cache, queues,
                            queryLogicFactory, predictor, publisher, metricFactory, metricClient);
        }
        
        @Bean
        public Queue<QueryRequest> queryRequests() {
            return new LinkedList<>();
        }
        
        @Bean
        public FindWorkTask findWorkTask(QueryStorageCache cache, QueryExecutor executor) {
            return new FindWorkTask(cache, executor, new FindWorkTask.CloseCancelCache(10), null, null);
        }
        
        @Bean
        public List<QueryMetricClient.Request> requests() {
            return new ArrayList<>();
        }
        
        @Bean
        public QueryMetricClient metricClient(final List<QueryMetricClient.Request> requests) {
            return new QueryMetricClient(new RestTemplateBuilder(), null, null, null, null) {
                
                @Override
                public void submit(Request request) throws Exception {
                    log.debug("Submitted request " + request);
                    requests.add(request);
                }
                
                @Override
                protected HttpEntity createRequestEntity(ProxiedUserDetails user, ProxiedUserDetails trustedUser, Object body) throws JsonProcessingException {
                    return null;
                }
            };
        }
        
        @Bean
        public QueryMetricFactory metricFactory() {
            return new QueryMetricFactoryImpl();
        }
        
        @Bean
        public LinkedList<RemoteQueryRequestEvent> testQueryRequestEvents() {
            return new LinkedList<>();
        }
        
        @Bean
        @Primary
        public QueryPredictor testQueryPredictor() {
            return new QueryPredictor() {
                @Override
                public Set<BaseQueryMetric.Prediction> predict(BaseQueryMetric query) throws PredictionException {
                    return predictions;
                }
            };
        }
        
        @Bean
        @Primary
        public ApplicationEventPublisher testPublisher() {
            return new ApplicationEventPublisher() {
                @Override
                public void publishEvent(ApplicationEvent event) {
                    saveEvent(event);
                }
                
                @Override
                public void publishEvent(Object event) {
                    saveEvent(event);
                }
                
                private void saveEvent(Object event) {
                    if (event instanceof RemoteQueryRequestEvent) {
                        testQueryRequestEvents().addLast(((RemoteQueryRequestEvent) event));
                    }
                }
            };
        }
        
        @Bean
        @Primary
        TestAccumuloSetup testAccumuloSetup() throws Exception {
            TestAccumuloSetup accumuloSetup = new TestAccumuloSetup();
            accumuloSetup.before();
            accumuloSetup.setData(FileType.CSV, dataType);
            synchronized (dataToCleanup) {
                dataToCleanup.add(accumuloSetup);
            }
            return accumuloSetup;
        }
        
        @Bean
        @Primary
        public Connector testConnector(TestAccumuloSetup accumuloSetup) throws Exception {
            return accumuloSetup.loadTables(log);
        }
        
        @Bean
        @Primary
        public AccumuloConnectionFactory testConnectionFactory(Connector connector) {
            return new AccumuloConnectionFactory() {
                
                @Override
                public void close() throws Exception {
                    
                }
                
                @Override
                public Connector getConnection(String userDN, Collection<String> proxyServers, Priority priority, Map<String,String> trackingMap)
                                throws Exception {
                    return connector;
                }
                
                @Override
                public Connector getConnection(String userDN, Collection<String> proxyServers, String poolName, Priority priority,
                                Map<String,String> trackingMap) throws Exception {
                    return connector;
                }
                
                @Override
                public void returnConnection(Connector connection) throws Exception {
                    
                }
                
                @Override
                public String report() {
                    return null;
                }
                
                @Override
                public List<ConnectionPool> getConnectionPools() {
                    return null;
                }
                
                @Override
                public int getConnectionUsagePercent() {
                    return 0;
                }
                
                @Override
                public Map<String,String> getTrackingMap(StackTraceElement[] stackTrace) {
                    return new HashMap<>();
                }
            };
        }
    }
    
    public static class TestQueryExecutor extends QueryExecutor {
        private final Queue<QueryRequest> requests;
        
        public TestQueryExecutor(Queue<QueryRequest> queryRequests, ExecutorProperties executorProperties, QueryProperties queryProperties,
                        BusProperties busProperties, ApplicationContext appCtx, AccumuloConnectionFactory connectionFactory, QueryStorageCache cache,
                        QueryResultsManager queues, QueryLogicFactory queryLogicFactory, QueryPredictor predictor, ApplicationEventPublisher publisher,
                        QueryMetricFactory metricFactory, QueryMetricClient metricClient) {
            super(executorProperties, queryProperties, busProperties, appCtx, connectionFactory, cache, queues, queryLogicFactory, predictor, publisher,
                            metricFactory, metricClient);
            this.requests = queryRequests;
        }
        
        @Override
        public void handleRemoteRequest(QueryRequest queryRequest, String originService, String destinationService) {
            requests.add(queryRequest);
        }
    }
    
}
