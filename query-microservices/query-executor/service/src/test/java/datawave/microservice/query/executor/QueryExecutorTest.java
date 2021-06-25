package datawave.microservice.query.executor;

import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.logic.CheckpointableQueryLogic;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.logic.QueryLogicFactory;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryCache;
import datawave.microservice.query.storage.QueryQueueListener;
import datawave.microservice.query.storage.QueryQueueManager;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskLockException;
import datawave.microservice.query.storage.TaskStates;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import datawave.security.util.DnUtils;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.TimeZone;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static datawave.query.testframework.RawDataManager.JEXL_AND_OP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ContextConfiguration(classes = QueryExecutorTest.QueryExecutorTestConfiguration.class)
public abstract class QueryExecutorTest {
    private static final Logger log = Logger.getLogger(QueryExecutorTest.class);
    
    public static DataTypeHadoopConfig dataType;
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
    private ApplicationContext publisher;
    
    @Autowired
    private QueryCache queryCache;
    
    @Autowired
    private QueryStorageCache storageService;
    
    @Autowired
    private QueryQueueManager queueManager;
    
    @Autowired
    private LinkedList<QueryRequest> queryRequests;
    
    protected Connector connector;
    private QueryExecutor queryExecutor;
    
    public String TEST_POOL = "TestPool";
    
    private Queue<QueryQueueListener> listeners = new LinkedList<>();
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
    
    @ActiveProfiles({"QueryExecutorTest", "sync-enabled", "send-notifications"})
    public static class LocalQueryExecutorTest extends QueryExecutorTest {}
    
    @EmbeddedKafka
    @ActiveProfiles({"QueryExecutorTest", "sync-enabled", "send-notifications", "use-embedded-kafka"})
    public static class EmbeddedKafkaQueryExecutorTest extends QueryExecutorTest {}
    
    @Disabled("Cannot run this test without an externally deployed RabbitMQ instance.")
    @ActiveProfiles({"QueryExecutorTest", "sync-enabled", "send-notifications", "use-rabbit"})
    public static class RabbitQueryExecutorTest extends QueryExecutorTest {}
    
    @Disabled("Cannot run this test without an externally deployed Kafka instance.")
    @ActiveProfiles({"QueryExecutorTest", "sync-enabled", "send-notifications", "use-kafka"})
    public static class KafkaQueryExecutorTest extends QueryExecutorTest {}
    
    @Configuration
    @Profile("QueryExecutorTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class QueryExecutorTestConfiguration {
        @Bean
        public LinkedList<QueryRequest> queryTaskNotifications() {
            return new LinkedList<>();
        }
        
        @Bean
        @Primary
        public ApplicationEventPublisher publisher(ApplicationEventPublisher publisher) {
            return event -> {
                if (event instanceof RemoteQueryRequestEvent)
                    queryTaskNotifications().push(((RemoteQueryRequestEvent) event).getRequest());
            };
        }
    }
    
    @SpringBootApplication(scanBasePackages = "datawave.microservice")
    public static class TestApplication {
        public static void main(String[] args) {
            SpringApplication.run(QueryExecutorTest.TestApplication.class, args);
        }
    }
    
    @BeforeAll
    public static void setupData() throws IOException, URISyntaxException {
        FieldConfig generic = new GenericCityFields();
        generic.addReverseIndexField(CitiesDataType.CityField.STATE.name());
        generic.addReverseIndexField(CitiesDataType.CityField.CONTINENT.name());
        dataType = new CitiesDataType(CitiesDataType.CityEntry.generic, generic);
    }
    
    @BeforeEach
    public void filterSetup() throws Exception {
        // Logger.getLogger(PrintUtility.class).setLevel(Level.DEBUG);
        
        accumuloSetup = new TestAccumuloSetup();
        accumuloSetup.beforeEach(null);
        accumuloSetup.setData(FileType.CSV, dataType);
        connector = accumuloSetup.loadTables(log);
        queryExecutor = new QueryExecutor(executorProperties, queryProperties, busProperties, connector, storageService, queueManager, queryLogicFactory,
                        publisher);
    }
    
    @AfterEach
    public void cleanup() throws Exception {
        while (!listeners.isEmpty()) {
            listeners.remove().stop();
        }
        while (!createdQueries.isEmpty()) {
            try {
                storageService.deleteQuery(createdQueries.remove());
            } catch (Exception e) {
                log.error("Failed to delete query", e);
            }
        }
        if (accumuloSetup != null) {
            accumuloSetup.afterEach(null);
            accumuloSetup = null;
        }
    }
    
    @DirtiesContext
    @Test
    public void testCheckpointableQuery() throws ParseException, InterruptedException, IOException, TaskLockException {
        // ensure the message queue is empty
        assertTrue(queryRequests.isEmpty());
        
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
        TaskKey key = storageService.createQuery(queryPool, query, Collections.singleton(CitiesDataType.getTestAuths()), 3);
        createdQueries.add(key.getQueryId());
        assertNotNull(key);
        
        TaskStates states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.READY, states.getState(key));
        
        // ensure we got a task notification
        QueryRequest notification = queryRequests.pop();
        assertNotNull(notification);
        assertEquals(key.getQueryId().toString(), notification.getQueryId());
        
        // pass the notification to the query executor
        QueryRequest request = QueryRequest.request(notification.getMethod(), notification.getQueryId());
        queryExecutor.handleRemoteRequest(request, true);
        // now we need to wait for its completion
        
        QueryStatus queryStatus = storageService.getQueryStatus(key.getQueryId());
        assertEquals(QueryStatus.QUERY_STATE.CREATED, queryStatus.getQueryState());
        assertEquals(expectPlan, queryStatus.getPlan());
        
        QueryQueueListener listener = queueManager.createListener("QueryExecutorTest.testCheckpointableQuery", key.getQueryId().toString());
        
        notification = queryRequests.poll();
        assertNotNull(notification);
        while (notification != null) {
            assertEquals(key.getQueryId(), notification.getQueryId());
            assertEquals(QueryRequest.Method.NEXT, notification.getMethod());
            
            request = QueryRequest.request(notification.getMethod(), notification.getQueryId());
            queryExecutor.handleRemoteRequest(request, true);
            
            queryStatus = storageService.getQueryStatus(key.getQueryId());
            assertEquals(QueryStatus.QUERY_STATE.CREATED, queryStatus.getQueryState());
            notification = queryRequests.poll();
        }
        
        // count the results
        int count = 0;
        while (listener.receive(0) != null) {
            count++;
        }
        assertTrue(count >= 1);
    }
    
    @DirtiesContext
    @Test
    public void testNonCheckpointableQuery() throws ParseException, InterruptedException, IOException, TaskLockException {
        // ensure the message queue is empty
        assertTrue(queryRequests.isEmpty());
        
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
        TaskKey key = storageService.createQuery(queryPool, query, Collections.singleton(CitiesDataType.getTestAuths()), 3);
        createdQueries.add(key.getQueryId());
        assertNotNull(key);
        
        TaskStates states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.READY, states.getState(key));
        
        // ensure we got a task notification
        QueryRequest notification = queryRequests.pop();
        assertNotNull(notification);
        assertEquals(key.getQueryId(), notification.getQueryId());
        
        // pass the notification to the query executor
        QueryExecutor queryExecutor = new QueryExecutor(executorProperties, queryProperties, busProperties, connector, storageService, queueManager,
                        new QueryLogicFactory() {
                            
                            @Override
                            public QueryLogic<?> getQueryLogic(String name, Collection<String> userRoles)
                                            throws QueryException, IllegalArgumentException, CloneNotSupportedException {
                                QueryLogic<?> queryLogic = queryLogicFactory.getQueryLogic(name, userRoles);
                                if (queryLogic instanceof CheckpointableQueryLogic) {
                                    ((CheckpointableQueryLogic) queryLogic).setCheckpointable(false);
                                }
                                return queryLogic;
                            }
                            
                            @Override
                            public QueryLogic<?> getQueryLogic(String name) throws QueryException, IllegalArgumentException, CloneNotSupportedException {
                                QueryLogic<?> queryLogic = queryLogicFactory.getQueryLogic(name);
                                if (queryLogic instanceof CheckpointableQueryLogic) {
                                    ((CheckpointableQueryLogic) queryLogic).setCheckpointable(false);
                                }
                                return queryLogic;
                            }
                            
                            @Override
                            public List<QueryLogic<?>> getQueryLogicList() {
                                return queryLogicFactory.getQueryLogicList();
                            }
                        }, publisher);
        QueryRequest request = QueryRequest.request(notification.getMethod(), notification.getQueryId());
        queryExecutor.handleRemoteRequest(request, true);
        
        QueryStatus queryStatus = storageService.getQueryStatus(key.getQueryId());
        assertEquals(QueryStatus.QUERY_STATE.CREATED, queryStatus.getQueryState());
        assertEquals(expectPlan, queryStatus.getPlan());
        
        QueryQueueListener listener = queueManager.createListener("QueryExecutorTest.testCheckpointableQuery", key.getQueryId().toString());
        
        notification = queryRequests.poll();
        assertNull(notification);
        
        // count the results
        int count = 0;
        while (listener.receive(0) != null) {
            count++;
        }
        assertTrue(count >= 1);
    }
    
    public class TestAccumuloSetup extends AccumuloSetup implements BeforeEachCallback, AfterEachCallback {
        
        @Override
        public void afterEach(ExtensionContext extensionContext) throws Exception {
            after();
        }
        
        @Override
        public void beforeEach(ExtensionContext extensionContext) throws Exception {
            try {
                before();
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }
}
