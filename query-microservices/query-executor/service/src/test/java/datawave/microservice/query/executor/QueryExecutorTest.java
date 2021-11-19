package datawave.microservice.query.executor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.Result;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
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
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
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
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TimeZone;
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
@ActiveProfiles("QueryExecutorTest")
@ContextConfiguration
public abstract class QueryExecutorTest {
    private static final Logger log = Logger.getLogger(QueryExecutorTest.class);
    
    public static DataTypeHadoopConfig dataType;
    public static TestAccumuloSetup accumuloSetup;
    
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
    private QueryExecutor queryExecutor;
    
    @Autowired
    private QueryStorageCache storageService;
    
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
    
    public String TEST_POOL = "TestPool";
    
    private Queue<QueryResultsListener> listeners = new LinkedList<>();
    private Queue<String> createdQueries = new LinkedList<>();
    
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
    }
    
    @ActiveProfiles({"QueryExecutorTest", "use-test"})
    public static class LocalQueryExecutorTest extends QueryExecutorTest {}
    
    @EmbeddedKafka
    @ActiveProfiles({"QueryExecutorTest", "use-embedded-kafka"})
    public static class EmbeddedKafkaQueryExecutorTest extends QueryExecutorTest {}
    
    @Disabled("Cannot run this test without an externally deployed RabbitMQ instance.")
    @ActiveProfiles({"QueryExecutorTest", "use-rabbit"})
    public static class RabbitQueryExecutorTest extends QueryExecutorTest {}
    
    @Disabled("Cannot run this test without an externally deployed Kafka instance.")
    @ActiveProfiles({"QueryExecutorTest", "use-kafka"})
    public static class KafkaQueryExecutorTest extends QueryExecutorTest {}
    
    @BeforeAll
    public static void setupData() throws Exception {
        try {
            FieldConfig generic = new GenericCityFields();
            generic.addReverseIndexField(CitiesDataType.CityField.STATE.name());
            generic.addReverseIndexField(CitiesDataType.CityField.CONTINENT.name());
            dataType = new CitiesDataType(CitiesDataType.CityEntry.generic, generic);
            accumuloSetup = new TestAccumuloSetup();
            accumuloSetup.before();
            accumuloSetup.setData(FileType.CSV, dataType);
        } catch (Exception e) {
            log.error("Failed to setup data", e);
            throw e;
        }
    }
    
    @AfterAll
    public static void cleanupData() {
        accumuloSetup.after();
    }
    
    @AfterEach
    public void cleanup() throws Exception {
        while (!listeners.isEmpty()) {
            listeners.remove().close();
        }
        while (!createdQueries.isEmpty()) {
            try {
                storageService.deleteQuery(createdQueries.remove());
            } catch (Exception e) {
                log.error("Failed to delete query", e);
            }
        }
    }
    
    @DirtiesContext
    @Test
    public void testCheckpointableQuery() throws Exception {
        // ensure the message queue is empty
        assertTrue(queryRequestsEvents.isEmpty());
        
        String city = "rome";
        String country = "italy";
        String queryStr = CitiesDataType.CityField.CITY.name() + ":\"" + city + "\"" + AND_OP + "#EVALUATION_ONLY('" + CitiesDataType.CityField.COUNTRY.name()
                        + ":\"" + country + "\"')";
        
        String expectPlan = CitiesDataType.CityField.CITY.name() + EQ_OP + "'" + city + "'" + JEXL_AND_OP + "((_Eval_ = true) && "
                        + CitiesDataType.CityField.COUNTRY.name() + EQ_OP + "'" + country + "')";
        
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
        createdQueries.add(key.getQueryId());
        
        QueryStatus queryStatusTest = storageService.getQueryStatus(key.getQueryId());
        
        TaskStates states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.READY, states.getState(key.getTaskId()));
        
        // pass a create request to the executor
        QueryRequest request = QueryRequest.create(key.getQueryId());
        queryExecutor.handleRemoteRequest(request, "query:**", "executor-" + TEST_POOL + ":**");
        
        // wait for the create task to finish
        long startTime = System.currentTimeMillis();
        while (queryRequestsEvents.isEmpty() && (System.currentTimeMillis() - startTime) < TimeUnit.SECONDS.toMillis(5)) {
            Thread.sleep(100);
        }
        
        // the first event should be the create response from the executor to the query service
        RemoteQueryRequestEvent notification = queryRequestsEvents.removeFirst();
        assertNotNull(notification);
        assertEquals("query:**", notification.getDestinationService());
        assertTrue(notification.getOriginService().startsWith("executor-" + TEST_POOL + ":"));
        assertEquals(QueryRequest.Method.CREATE, notification.getRequest().getMethod());
        assertEquals(key.getQueryId(), notification.getRequest().getQueryId());
        
        QueryStatus queryStatus = storageService.getQueryStatus(key.getQueryId());
        assertEquals(QueryStatus.QUERY_STATE.CREATED, queryStatus.getQueryState());
        assertEquals(expectPlan, queryStatus.getPlan());
        
        // now we will wait for all next tasks to be generated
        startTime = System.currentTimeMillis();
        while (storageService.getTaskStates(key.getQueryId()).isCreatingTasks() && (System.currentTimeMillis() - startTime) < TimeUnit.SECONDS.toMillis(5)) {
            Thread.sleep(250);
        }
        
        QueryResultsListener listener = queueManager.createListener("QueryExecutorTest.testCheckpointableQuery", key.getQueryId());
        
        List<RemoteQueryRequestEvent> eventsToRemove = new ArrayList<>();
        
        // process each of the next requests
        for (RemoteQueryRequestEvent event : queryRequestsEvents) {
            assertNotNull(event);
            assertEquals("executor-" + TEST_POOL + ":**", event.getDestinationService());
            assertTrue(event.getOriginService().startsWith("executor-" + TEST_POOL + ":"));
            assertEquals(QueryRequest.Method.NEXT, event.getRequest().getMethod());
            assertEquals(key.getQueryId(), event.getRequest().getQueryId());
            
            // process the next event, generating results
            queryExecutor.handleRemoteRequest(event.getRequest(), event.getOriginService(), event.getDestinationService());
            
            eventsToRemove.add(event);
        }
        
        queryRequestsEvents.removeAll(eventsToRemove);
        assertEquals(0, queryRequestsEvents.size());
        
        // wait for all next requests to be finished
        startTime = System.currentTimeMillis();
        while (storageService.getTaskStates(key.getQueryId()).hasUnfinishedTasks()
                        && (System.currentTimeMillis() - startTime) < TimeUnit.SECONDS.toMillis(30)) {
            Thread.sleep(250);
        }
        
        // get a result
        startTime = System.currentTimeMillis();
        Result result = null;
        while (result == null && (System.currentTimeMillis() - startTime) < TimeUnit.SECONDS.toMillis(10)) {
            result = listener.receive(100, TimeUnit.MILLISECONDS);
        }
        assertNotNull(result);
        assertFalse(requests.isEmpty());
    }
    
    @DirtiesContext
    @Test
    public void testNonCheckpointableQuery() throws Exception {
        // ensure the message queue is empty
        assertTrue(queryRequestsEvents.isEmpty());
        
        String city = "rome";
        String country = "italy";
        String queryStr = CitiesDataType.CityField.CITY.name() + ":\"" + city + "\"" + AND_OP + "#EVALUATION_ONLY('" + CitiesDataType.CityField.COUNTRY.name()
                        + ":\"" + country + "\"')";
        
        String expectPlan = CitiesDataType.CityField.CITY.name() + EQ_OP + "'" + city + "'" + JEXL_AND_OP + "((_Eval_ = true) && "
                        + CitiesDataType.CityField.COUNTRY.name() + EQ_OP + "'" + country + "')";
        
        Query query = new QueryImpl();
        query.setQuery(queryStr);
        query.setQueryLogicName("LegacyEventQuery");
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
        createdQueries.add(key.getQueryId());
        assertNotNull(key);
        
        QueryResultsListener listener = queueManager.createListener("QueryExecutorTest.testCheckpointableQuery", key.getQueryId());
        
        TaskStates states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.READY, states.getState(key.getTaskId()));
        
        // pass a create request to the executor
        QueryRequest request = QueryRequest.create(key.getQueryId());
        queryExecutor.handleRemoteRequest(request, "query:**", "executor-" + TEST_POOL + ":**");
        
        // wait for the create task to finish
        long startTime = System.currentTimeMillis();
        while (queryRequestsEvents.isEmpty() && (System.currentTimeMillis() - startTime) < TimeUnit.SECONDS.toMillis(5)) {
            Thread.sleep(100);
        }
        
        // the first event should be the create response from the executor to the query service
        RemoteQueryRequestEvent notification = queryRequestsEvents.removeFirst();
        assertNotNull(notification);
        assertEquals("query:**", notification.getDestinationService());
        assertTrue(notification.getOriginService().startsWith("executor-" + TEST_POOL + ":"));
        assertEquals(QueryRequest.Method.CREATE, notification.getRequest().getMethod());
        assertEquals(key.getQueryId(), notification.getRequest().getQueryId());
        
        QueryStatus queryStatus = storageService.getQueryStatus(key.getQueryId());
        assertEquals(QueryStatus.QUERY_STATE.CREATED, queryStatus.getQueryState());
        assertEquals(expectPlan, queryStatus.getPlan());
        
        TaskStates taskStates = storageService.getTaskStates(key.getQueryId());
        
        // wait for the create task to finish
        startTime = System.currentTimeMillis();
        while ((taskStates.hasUnfinishedTasks() || taskStates.isCreatingTasks()) && (System.currentTimeMillis() - startTime) < TimeUnit.SECONDS.toMillis(45)) {
            Thread.sleep(250);
            taskStates = storageService.getTaskStates(key.getQueryId());
        }
        
        notification = queryRequestsEvents.poll();
        assertNull(notification);
        
        states = storageService.getTaskStates(key.getQueryId());
        assertTrue(states.hasTasksForState(TaskStates.TASK_STATE.COMPLETED));
        assertFalse(states.hasUnfinishedTasks());
        queryStatus = storageService.getQueryStatus(key.getQueryId());
        assertEquals(QueryStatus.QUERY_STATE.CREATED, queryStatus.getQueryState());
        
        // get a result
        startTime = System.currentTimeMillis();
        Result result = null;
        while (result == null && (System.currentTimeMillis() - startTime) < TimeUnit.SECONDS.toMillis(10)) {
            result = listener.receive(100, TimeUnit.MILLISECONDS);
        }
        assertNotNull(result);
        assertFalse(requests.isEmpty());
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
    @Profile("QueryExecutorTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class QueryExecutorTestConfiguration {
        @Bean
        public HazelcastInstance hazelcastInstance() {
            return Hazelcast.newHazelcastInstance();
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
        public Connector testConnector() throws Exception {
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
    
}
