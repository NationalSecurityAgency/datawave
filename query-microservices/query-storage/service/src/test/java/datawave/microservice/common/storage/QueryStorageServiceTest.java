package datawave.microservice.common.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.common.storage.config.QueryStorageConfig;
import datawave.microservice.common.storage.config.QueryStorageProperties;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryParametersImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryStorageServiceTest", "sync-disabled"})
public class QueryStorageServiceTest {
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    private JWTRestTemplate jwtRestTemplate;
    
    @Autowired
    private QueryStorageProperties queryStorageProperties;
    
    @Autowired
    private QueryStorageService storageService;
    
    @Autowired
    private QueryStorageStateService storageStateService;
    
    @Autowired
    private QueryStorageConfig.TaskNotificationSourceBinding taskNotificationSourceBinding;
    
    @Autowired
    @Qualifier(QueryStorageConfig.TaskNotificationSourceBinding.NAME)
    private MessageChannel taskNotificationChannel;
    
    @Autowired
    private MessageCollector messageCollector;
    
    private SubjectIssuerDNPair DN;
    private String userDN = "userDn";
    
    @Before
    public void setup() {
        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        DN = SubjectIssuerDNPair.of(userDN, "issuerDn");
    }
    
    @After
    public void cleanup() throws InterruptedException {
        storageService.clear();
        messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).clear();
    }
    
    @Test
    public void testStoreQuery() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        QueryPool queryPool = new QueryPool("default");
        TaskKey key = storageService.storeQuery(queryPool, query);
        assertNotNull(key);
        
        // ensure we got a task notification
        assertFalse(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        QueryTaskNotification notification = (QueryTaskNotification) messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).take()
                        .getPayload();
        assertEquals(key, notification.getTaskKey());
        
        // ensure the message queue is empty again
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        QueryTask task = storageService.getTask(key);
        assertQueryTask(key, QueryTask.QUERY_ACTION.CREATE, query, task);
    }
    
    @Test
    public void testStoreTask() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool("default");
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryId, queryPool, query);
        QueryTask task = storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        TaskKey key = task.getTaskKey();
        assertEquals(checkpoint.getQueryKey(), key);
        
        // ensure we got a task notification
        assertFalse(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        QueryTaskNotification notification = (QueryTaskNotification) messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).take()
                        .getPayload();
        assertEquals(key, notification.getTaskKey());
        
        // ensure the message queue is empty again
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        task = storageService.getTask(key);
        assertQueryTask(key, QueryTask.QUERY_ACTION.NEXT, query, task);
    }
    
    @Test
    public void testCheckpointTask() throws InterruptedException, ParseException {
        // ensure the message queue is empty
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool("default");
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryId, queryPool, query);
        
        TaskKey key = new TaskKey(UUID.randomUUID(), queryPool, UUID.randomUUID());
        try {
            storageService.checkpointTask(key, checkpoint);
            fail("Expected storage service to fail checkpointing an invalid task ofkey");
        } catch (IllegalArgumentException e) {
            // expected
        }
        
        key = new TaskKey(UUID.randomUUID(), checkpoint.getQueryKey());
        try {
            storageService.checkpointTask(key, checkpoint);
            fail("Expected storage service to fail checkpointing a missing task");
        } catch (NullPointerException e) {
            // expected
        }
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = (QueryTaskNotification) messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).take()
                        .getPayload();
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        // now update the task
        Map<String,Object> props = new HashMap<>();
        props.put("checkpoint", Boolean.TRUE);
        checkpoint = new QueryCheckpoint(queryId, queryPool, props);
        storageService.checkpointTask(notification.getTaskKey(), checkpoint);
        
        // ensure we did not get another task notification
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        QueryTask task = storageService.getTask(notification.getTaskKey());
        assertEquals(checkpoint, task.getQueryCheckpoint());
    }
    
    @Test
    public void testGetAndDeleteTask() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool("default");
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryId, queryPool, query);
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = (QueryTaskNotification) messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).take()
                        .getPayload();
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        // ensure we can get a task
        QueryTask task = storageService.getTask(notification.getTaskKey());
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the task
        storageService.deleteTask(notification.getTaskKey());
        
        // ensure we did not get another task notification
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        // ensure there is no more task stored
        task = storageService.getTask(notification.getTaskKey());
        assertNull(task);
    }
    
    @Test
    public void testGetAndDeleteQueryTasks() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool("default");
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryId, queryPool, query);
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = (QueryTaskNotification) messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).take()
                        .getPayload();
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        // not get the query tasks
        List<QueryTask> tasks = storageService.getTasks(queryId);
        assertEquals(1, tasks.size());
        QueryTask task = tasks.get(0);
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQuery(queryId);
        
        // ensure we did not get another task notification
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        // make sure it deleted
        tasks = storageService.getTasks(queryId);
        assertEquals(0, tasks.size());
    }
    
    @Test
    public void testGetAndDeleteTypeTasks() throws ParseException, InterruptedException {
        // ensure the message queue is empty
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        UUID queryId = UUID.randomUUID();
        QueryPool queryPool = new QueryPool("default");
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryId, queryPool, query);
        
        storageService.createTask(QueryTask.QUERY_ACTION.NEXT, checkpoint);
        
        // clear out message queue
        QueryTaskNotification notification = (QueryTaskNotification) messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).take()
                        .getPayload();
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        // not get the query tasks
        List<QueryTask> tasks = storageService.getTasks(queryPool);
        assertEquals(1, tasks.size());
        QueryTask task = tasks.get(0);
        assertQueryTask(notification.getTaskKey(), QueryTask.QUERY_ACTION.NEXT, query, task);
        
        // now delete the query tasks
        storageService.deleteQueryPool(queryPool);
        
        // ensure we did not get another task notification
        assertTrue(messageCollector.forChannel(taskNotificationSourceBinding.queryTaskSource()).isEmpty());
        
        // make sure it deleted
        tasks = storageService.getTasks(queryPool);
        assertEquals(0, tasks.size());
    }
    
    @Test
    public void testStateStorageService() throws ParseException, InterruptedException {
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        QueryPool queryPool = new QueryPool("default");
        TaskKey key = storageService.storeQuery(queryPool, query);
        assertNotNull(key);
        
        QueryStorageStateService storageStateService = new TestQueryStateService("Administrator");
        
        QueryState state = storageStateService.getQuery(key.getQueryId().toString());
        assertQueryCreate(key.getQueryId(), queryPool, state);
        
        List<QueryState> queries = storageStateService.getRunningQueries();
        assertEquals(1, queries.size());
        assertQueryCreate(key.getQueryId(), queryPool, queries.get(0));
        
        queries = storageStateService.getRunningQueries(queryPool.toString());
        assertEquals(1, queries.size());
        assertQueryCreate(key.getQueryId(), queryPool, queries.get(0));
        
        List<TaskDescription> tasks = storageStateService.getTasks(key.getQueryId().toString());
        assertEquals(1, tasks.size());
        assertQueryCreate(key.getQueryId(), queryPool, query, tasks.get(0));
        
        QueryTask task = storageService.getTask(key);
        assertQueryTask(key, QueryTask.QUERY_ACTION.CREATE, query, task);
    }
    
    private void assertQueryCreate(UUID queryId, QueryPool queryPool, QueryState state) {
        assertEquals(queryId, state.getQueryId());
        assertEquals(queryPool, state.getQueryPool());
        Map<QueryTask.QUERY_ACTION,Integer> counts = state.getTaskCounts();
        assertEquals(1, counts.size());
        assertTrue(counts.containsKey(QueryTask.QUERY_ACTION.CREATE));
        assertEquals(1, counts.get(QueryTask.QUERY_ACTION.CREATE).intValue());
    }
    
    private void assertQueryCreate(UUID queryId, QueryPool queryPool, Query query, TaskDescription task) throws ParseException {
        assertNotNull(task.getTaskKey());
        assertEquals(queryId, task.getTaskKey().getQueryId());
        assertEquals(queryPool, task.getTaskKey().getQueryPool());
        assertEquals(QueryTask.QUERY_ACTION.CREATE, task.getAction());
        assertEquals(query.getQuery(), task.getParameters().get(QueryImpl.QUERY));
        assertEquals(QueryParametersImpl.formatDate(query.getBeginDate()), task.getParameters().get(QueryImpl.BEGIN_DATE));
        assertEquals(QueryParametersImpl.formatDate(query.getEndDate()), task.getParameters().get(QueryImpl.END_DATE));
    }
    
    private void assertQueryTask(TaskKey key, QueryTask.QUERY_ACTION action, Query query, QueryTask task) throws ParseException {
        assertEquals(key, task.getTaskKey());
        assertEquals(action, task.getAction());
        assertEquals(task.getQueryCheckpoint().getQueryKey(), key);
        assertEquals(query, task.getQueryCheckpoint().getPropertiesAsQuery());
    }
    
    /**
     * This is a query state service implementation invokes the method by using the webservice hosted on a local port using the role provided at construction.
     */
    private class TestQueryStateService implements QueryStorageStateService {
        private ProxiedUserDetails authUser;
        
        public TestQueryStateService(String asRole) {
            Collection<String> roles = Collections.singleton("Administrator");
            DatawaveUser uathDWUser = new DatawaveUser(DN, USER, null, roles, null, System.currentTimeMillis());
            authUser = new ProxiedUserDetails(Collections.singleton(uathDWUser), uathDWUser.getCreationTime());
        }
        
        @Override
        public List<QueryState> getRunningQueries() {
            UriComponents getQueryUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort)
                            .path("/QueryStorage/v1/queries").build();
            return toStates(jwtRestTemplate.exchange(authUser, HttpMethod.GET, getQueryUri, String.class));
        }
        
        @Override
        public QueryState getQuery(String queryId) {
            UriComponents getQueryUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort)
                            .path("/QueryStorage/v1/query/" + queryId).build();
            return toState(jwtRestTemplate.exchange(authUser, HttpMethod.GET, getQueryUri, String.class));
        }
        
        @Override
        public List<TaskDescription> getTasks(String queryId) {
            UriComponents getQueryUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort)
                            .path("/QueryStorage/v1/tasks/" + queryId).build();
            return toTaskDescriptions(jwtRestTemplate.exchange(authUser, HttpMethod.GET, getQueryUri, String.class));
            
        }
        
        @Override
        public List<QueryState> getRunningQueries(String queryPool) {
            UriComponents getQueryUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort)
                            .path("/QueryStorage/v1/queries/" + queryPool).build();
            return toStates(jwtRestTemplate.exchange(authUser, HttpMethod.GET, getQueryUri, String.class));
        }
        
        private final ObjectMapper mapper = new ObjectMapper();
        
        private QueryState toState(ResponseEntity<String> responseEntity) {
            try {
                return mapper.readerFor(QueryState.class).readValue(responseEntity.getBody());
            } catch (IOException e) {
                throw new RuntimeException("Failed to decode value " + responseEntity.getBody(), e);
            }
        }
        
        private List<QueryState> toStates(ResponseEntity<String> responseEntity) {
            try {
                return mapper.readValue(responseEntity.getBody(), new TypeReference<List<QueryState>>() {});
            } catch (IOException e) {
                throw new RuntimeException("Failed to decode value " + responseEntity.getBody(), e);
            }
        }
        
        private List<TaskDescription> toTaskDescriptions(ResponseEntity<String> responseEntity) {
            try {
                return mapper.readValue(responseEntity.getBody(), new TypeReference<List<TaskDescription>>() {});
            } catch (IOException e) {
                throw new RuntimeException("Failed to decode value " + responseEntity.getBody(), e);
            }
        }
    }
    
}
