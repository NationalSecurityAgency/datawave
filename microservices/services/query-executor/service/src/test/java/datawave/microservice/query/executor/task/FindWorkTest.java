package datawave.microservice.query.executor.task;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
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

import org.apache.accumulo.core.client.AccumuloClient;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.result.ConnectionPool;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.core.query.predict.QueryPredictor;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
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
    protected Map<String,Queue<QueryRequest>> queryRequests;
    
    @Autowired
    protected FindWorkTask findWorkTask;
    
    public String TEST_POOL = "TestPool";
    
    public static HashSet<BaseQueryMetric.Prediction> predictions = Sets.newHashSet(new BaseQueryMetric.Prediction("Success", 0.5d),
                    new BaseQueryMetric.Prediction("Elephants", 0.0d));
    
    static {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        System.setProperty("file.encoding", StandardCharsets.UTF_8.name());
        // System.setProperty(DnUtils.NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
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
    
    public Query getQuery() throws ParseException {
        String city = "rome";
        String country = "italy";
        String queryStr = CitiesDataType.CityField.CITY.name() + ":\"" + city + "\"" + AND_OP + "#EVALUATION_ONLY('" + CitiesDataType.CityField.COUNTRY.name()
                        + ":\"" + country + "\"')";
        
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
        return query;
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
    
    private void testCreateOrphans(TaskStates.TASK_STATE initialState, QueryStatus.CREATE_STAGE initialStage, TaskStates.TASK_STATE expectedState,
                    QueryRequest.Method expectedAction) throws Exception {
        TaskKey key = storageService.createQuery(TEST_POOL, getQuery(), null, Collections.singleton(CitiesDataType.getTestAuths()), 20);
        assertNotNull(key);
        
        // now set the initial stage
        storageService.updateCreateStage(key.getQueryId(), initialStage);
        
        // and test
        testOrphans(key, initialState, expectedState, expectedAction);
    }
    
    private void testOrphans(TaskKey key, TaskStates.TASK_STATE initialState, TaskStates.TASK_STATE expectedState, QueryRequest.Method expectedAction)
                    throws Exception {
        assertNotNull(key);
        
        // now put the task in a running state in the PLAN stage
        storageService.updateTaskState(key, initialState);
        
        // wait until the timeout
        Thread.sleep(2 * executorProperties.getOrphanThresholdMs());
        
        // execute the find work task to reset the now orphaned task
        findWorkTask.call();
        
        // verify the task has been set to an expected state
        TaskStates states = storageService.getTaskStates(key.getQueryId());
        assertEquals(expectedState, states.getState(key.getTaskId()));
        
        // Should have an expected request
        assertEquals(1, queryRequests.get(key.getQueryId()).size());
        QueryRequest request = queryRequests.get(key.getQueryId()).poll();
        assertEquals(key.getQueryId(), request.getQueryId());
        assertEquals(expectedAction, request.getMethod());
    }
    
    @Test
    public void testFindNoOrphans() throws Exception {
        testCreateOrphans(TaskStates.TASK_STATE.READY, QueryStatus.CREATE_STAGE.CREATE, TaskStates.TASK_STATE.READY, QueryRequest.Method.CREATE);
    }
    
    @Test
    public void testOrphanedCreateCreate() throws Exception {
        testCreateOrphans(TaskStates.TASK_STATE.RUNNING, QueryStatus.CREATE_STAGE.CREATE, TaskStates.TASK_STATE.READY, QueryRequest.Method.CREATE);
    }
    
    @Test
    public void testOrphanedCreatePlan() throws Exception {
        testCreateOrphans(TaskStates.TASK_STATE.RUNNING, QueryStatus.CREATE_STAGE.PLAN, TaskStates.TASK_STATE.READY, QueryRequest.Method.CREATE);
    }
    
    @Test
    public void testOrphanedCreateTask() throws Exception {
        testCreateOrphans(TaskStates.TASK_STATE.RUNNING, QueryStatus.CREATE_STAGE.TASK, TaskStates.TASK_STATE.FAILED, QueryRequest.Method.NEXT);
    }
    
    @Test
    public void testOrphanedCreateResults() throws Exception {
        testCreateOrphans(TaskStates.TASK_STATE.RUNNING, QueryStatus.CREATE_STAGE.RESULTS, TaskStates.TASK_STATE.COMPLETED, QueryRequest.Method.NEXT);
    }
    
    @Test
    public void testOrphanedNext() throws Exception {
        TaskKey key = storageService.createQuery(TEST_POOL, getQuery(), null, Collections.singleton(CitiesDataType.getTestAuths()), 20);
        assertNotNull(key);
        
        // Assume the create task is complete
        storageService.updateTaskState(key, TaskStates.TASK_STATE.COMPLETED);
        storageService.updateCreateStage(key.getQueryId(), QueryStatus.CREATE_STAGE.RESULTS);
        
        // create a next task
        QueryTask task = storageService.createTask(QueryRequest.Method.NEXT, new QueryCheckpoint(key.getQueryKey()));
        testOrphans(task.getTaskKey(), TaskStates.TASK_STATE.RUNNING, TaskStates.TASK_STATE.READY, QueryRequest.Method.NEXT);
    }
    
    @Test
    public void testOrphanedPlan() throws Exception {
        TaskKey key = storageService.planQuery(TEST_POOL, getQuery(), null, Collections.singleton(CitiesDataType.getTestAuths()));
        testOrphans(key, TaskStates.TASK_STATE.RUNNING, TaskStates.TASK_STATE.READY, QueryRequest.Method.PLAN);
    }
    
    @Test
    public void testOrphanedPredict() throws Exception {
        TaskKey key = storageService.predictQuery(TEST_POOL, getQuery(), null, Collections.singleton(CitiesDataType.getTestAuths()));
        testOrphans(key, TaskStates.TASK_STATE.RUNNING, TaskStates.TASK_STATE.READY, QueryRequest.Method.PREDICT);
    }
    
    private void checkFailed(String queryId) {
        QueryStatus status = storageService.getQueryStatus(queryId);
        if (status.getQueryState() == QueryStatus.QUERY_STATE.FAIL) {
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
        public QueryExecutor executor(Map<String,Queue<QueryRequest>> queryRequests, ExecutorProperties executorProperties, QueryProperties queryProperties,
                        BusProperties busProperties, ApplicationContext appCtx, AccumuloConnectionFactory connectionFactory, QueryStorageCache cache,
                        QueryResultsManager queues, QueryLogicFactory queryLogicFactory, QueryPredictor predictor, ApplicationEventPublisher publisher,
                        QueryMetricFactory metricFactory, QueryMetricClient metricClient) {
            // lets avoid waiting too long for these tests.
            executorProperties.setOrphanThresholdMs(100L);
            return new TestQueryExecutor(queryRequests, executorProperties, queryProperties, busProperties, appCtx, connectionFactory, cache, queues,
                            queryLogicFactory, predictor, publisher, metricFactory, metricClient);
        }
        
        @Bean
        public Map<String,Queue<QueryRequest>> queryRequests() {
            return new HashMap<>();
        }
        
        @Bean
        public FindWorkTask findWorkTask(QueryStorageCache cache, QueryExecutor executor) {
            return new FindWorkTask(cache, executor, null, null, new ExecutorStatusLogger());
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
                protected HttpEntity createRequestEntity(DatawaveUserDetails user, DatawaveUserDetails trustedUser, Object body)
                                throws JsonProcessingException {
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
        public AccumuloClient testClient(TestAccumuloSetup accumuloSetup) throws Exception {
            return accumuloSetup.loadTables(log);
        }
        
        @Bean
        @Primary
        public AccumuloConnectionFactory testConnectionFactory(AccumuloClient client) {
            return new AccumuloConnectionFactory() {
                
                @Override
                public void close() throws Exception {
                    
                }
                
                @Override
                public AccumuloClient getClient(String userDN, Collection<String> proxyServers, Priority priority, Map<String,String> trackingMap)
                                throws Exception {
                    return client;
                }
                
                @Override
                public AccumuloClient getClient(String userDN, Collection<String> proxyServers, String poolName, Priority priority,
                                Map<String,String> trackingMap) throws Exception {
                    return client;
                }
                
                @Override
                public void returnClient(AccumuloClient client) throws Exception {
                    
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
        private final Map<String,Queue<QueryRequest>> requests;
        
        public TestQueryExecutor(Map<String,Queue<QueryRequest>> queryRequests, ExecutorProperties executorProperties, QueryProperties queryProperties,
                        BusProperties busProperties, ApplicationContext appCtx, AccumuloConnectionFactory connectionFactory, QueryStorageCache cache,
                        QueryResultsManager queues, QueryLogicFactory queryLogicFactory, QueryPredictor predictor, ApplicationEventPublisher publisher,
                        QueryMetricFactory metricFactory, QueryMetricClient metricClient) {
            super(executorProperties, queryProperties, busProperties, appCtx, connectionFactory, cache, queues, queryLogicFactory, predictor, publisher,
                            metricFactory, metricClient);
            this.requests = queryRequests;
        }
        
        @Override
        public void handleRemoteRequest(QueryRequest queryRequest, String originService, String destinationService) {
            synchronized (requests) {
                Queue<QueryRequest> queue = requests.get(queryRequest.getQueryId());
                if (queue == null) {
                    queue = new LinkedList<>();
                    requests.put(queryRequest.getQueryId(), queue);
                }
                queue.offer(queryRequest);
            }
        }
    }
    
}
