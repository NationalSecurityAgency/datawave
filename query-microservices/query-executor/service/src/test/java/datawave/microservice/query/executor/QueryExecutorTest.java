package datawave.microservice.query.executor;

import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.microservice.common.storage.QueryCache;
import datawave.microservice.common.storage.QueryPool;
import datawave.microservice.common.storage.QueryQueueListener;
import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.QueryStatus;
import datawave.microservice.common.storage.QueryStorageCache;
import datawave.microservice.common.storage.QueryTaskNotification;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.TaskLockException;
import datawave.microservice.common.storage.TaskStates;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.logic.QueryLogicFactory;
import datawave.query.AnyFieldQueryTest;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;
import datawave.security.util.DnUtils;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.bus.event.RemoteQueryTaskNotificationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.TimeZone;
import java.util.UUID;

import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.EQ_OP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ContextConfiguration(classes = QueryExecutorTest.QueryExecutorTestConfiguration.class)
public abstract class QueryExecutorTest {
    private static final Logger log = Logger.getLogger(AnyFieldQueryTest.class);
    
    public TestAccumuloSetup accumuloSetup;
    
    @Autowired
    private QueryLogicFactory queryLogicFactory;
    
    @Autowired
    private ExecutorProperties executorProperties;
    
    @Autowired
    private QueryCache queryCache;
    
    @Autowired
    private QueryStorageCache storageService;
    
    @Autowired
    private QueryQueueManager queueManager;
    
    @Autowired
    private LinkedList<QueryTaskNotification> queryTaskNotifications;
    
    protected Connector connector;
    private QueryExecutor queryExecutor;
    
    public String TEST_POOL = "TestPool";
    
    private Queue<QueryQueueListener> listeners = new LinkedList<>();
    private Queue<UUID> createdQueries = new LinkedList<>();
    
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
        public LinkedList<QueryTaskNotification> queryTaskNotifications() {
            return new LinkedList<>();
        }
        
        @Bean
        @Primary
        public ApplicationEventPublisher publisher(ApplicationEventPublisher publisher) {
            return event -> {
                if (event instanceof RemoteQueryTaskNotificationEvent)
                    queryTaskNotifications().push(((RemoteQueryTaskNotificationEvent) event).getNotification());
            };
        }
    }
    
    @SpringBootApplication(scanBasePackages = "datawave.microservice")
    public static class TestApplication {
        public static void main(String[] args) {
            SpringApplication.run(QueryExecutorTest.TestApplication.class, args);
        }
    }
    
    @BeforeEach
    public void filterSetup() throws Exception {
        FieldConfig generic = new GenericCityFields();
        generic.addReverseIndexField(CitiesDataType.CityField.STATE.name());
        generic.addReverseIndexField(CitiesDataType.CityField.CONTINENT.name());
        DataTypeHadoopConfig dataType = new CitiesDataType(CitiesDataType.CityEntry.generic, generic);
        
        accumuloSetup = new TestAccumuloSetup();
        accumuloSetup.beforeEach(null);
        accumuloSetup.setData(FileType.CSV, dataType);
        connector = accumuloSetup.loadTables(log);
        queryExecutor = new QueryExecutor(executorProperties, connector, storageService, queueManager, queryLogicFactory);
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
    
    // @DirtiesContext
    // @Test
    public void testCreateQuery() throws ParseException, InterruptedException, IOException, TaskLockException {
        // ensure the message queue is empty
        assertTrue(queryTaskNotifications.isEmpty());
        
        String city = "rome";
        String state = "lazio";
        String queryStr = CitiesDataType.CityField.CITY.name() + ":\"" + city + "\"" + AND_OP + CitiesDataType.CityField.STATE.name() + ":\"" + state + "\"";
        String expect = CitiesDataType.CityField.CITY.name() + '_' + CitiesDataType.CityField.STATE.name() + EQ_OP + "'" + city
                        + CompositeIngest.DEFAULT_SEPARATOR + state + "'";
        
        Query query = new QueryImpl();
        query.setQuery(queryStr);
        query.setQueryLogicName("EventQuery");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        query.setQueryAuthorizations(CitiesDataType.getTestAuths().toString());
        query.setQueryName("TestQuery");
        query.setDnList(Collections.singletonList("test user"));
        query.setUserDN("test user");
        query.setPagesize(100);
        QueryPool queryPool = new QueryPool(TEST_POOL);
        TaskKey key = storageService.createQuery(queryPool, query, Collections.singleton(CitiesDataType.getTestAuths()), 3);
        createdQueries.add(key.getQueryId());
        assertNotNull(key);
        
        TaskStates states = storageService.getTaskStates(key.getQueryId());
        assertEquals(TaskStates.TASK_STATE.READY, states.getState(key));
        
        // ensure we got a task notification
        QueryTaskNotification notification = queryTaskNotifications.pop();
        assertNotNull(notification);
        assertEquals(key, notification.getTaskKey());
        
        // pass the notification to the query executor
        queryExecutor.handleQueryTaskNotification(notification);
        
        QueryStatus queryStatus = storageService.getQueryStatus(key.getQueryId());
        assertEquals(QueryStatus.QUERY_STATE.CREATED, queryStatus.getQueryState());
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
