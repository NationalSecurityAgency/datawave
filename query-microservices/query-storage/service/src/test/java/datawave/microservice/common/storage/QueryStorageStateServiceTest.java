package datawave.microservice.common.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.storage.QueryState;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskDescription;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskLockException;
import datawave.microservice.query.storage.TaskStates;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryStorageStateServiceTest"})
@EnableRabbit
public class QueryStorageStateServiceTest {
    
    @Configuration
    @Profile("QueryStorageStateServiceTest")
    @ComponentScan(basePackages = {"datawave.microservice"})
    public static class QueryStorageStateServiceTestConfiguration {}
    
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    private JWTRestTemplate jwtRestTemplate;
    
    @Autowired
    private QueryResultsManager queueManager;
    
    @Autowired
    private QueryStorageCache storageService;
    
    @Autowired
    private QueryStorageStateService storageStateService;
    
    private SubjectIssuerDNPair DN;
    private String userDN = "userDn";
    
    private static final String TEST_POOL = "storageTestPool";
    
    @BeforeEach
    public void setup() throws IOException {
        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        DN = SubjectIssuerDNPair.of(userDN, "issuerDn");
        storageService.clear();
    }
    
    @AfterEach
    public void cleanup() throws IOException {
        storageService.clear();
    }
    
    @Test
    public void testStateStorageService() throws ParseException, InterruptedException, TaskLockException, IOException {
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setQueryLogicName("EventQuery");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        String queryPool = TEST_POOL;
        Set<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations("FOO", "BAR"));
        TaskKey key = storageService.createQuery(queryPool, query, auths, 3);
        assertNotNull(key);
        QueryTask storedTask = storageService.getTask(key);
        assertNotNull(storedTask);
        List<TaskKey> storedTasks = storageService.getTasks(key.getQueryId());
        assertNotNull(storedTasks);
        assertEquals(1, storedTasks.size());
        
        QueryStorageStateService storageStateService = new TestQueryStateService("Administrator");
        
        QueryState state = storageStateService.getQuery(key.getQueryId().toString());
        assertQueryCreate(key.getQueryId(), queryPool, state);
        
        List<QueryState> queries = storageStateService.getRunningQueries();
        assertEquals(1, queries.size());
        assertQueryCreate(key.getQueryId(), queryPool, queries.get(0));
        
        List<TaskDescription> tasks = storageStateService.getTasks(key.getQueryId().toString());
        QueryStatus queryStatus = storageService.getQueryStatus(key.getQueryId());
        assertEquals(1, tasks.size());
        assertQueryCreate(key.getQueryId(), queryPool, query, tasks.get(0), queryStatus);
    }
    
    private void assertQueryCreate(String queryId, String queryPool, QueryState state) {
        assertEquals(queryId, state.getQueryStatus().getQueryKey().getQueryId());
        assertEquals(queryPool, state.getQueryStatus().getQueryKey().getQueryPool());
        TaskStates tasks = state.getTaskStates();
    }
    
    private void assertQueryCreate(String queryId, String queryPool, Query query, TaskDescription task, QueryStatus queryStatus) throws ParseException {
        assertNotNull(task.getTaskKey());
        assertEquals(queryId, task.getTaskKey().getQueryId());
        assertEquals(queryPool, task.getTaskKey().getQueryPool());
        assertEquals(query, queryStatus.getQuery());
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
                            .path("/query-storage-test/v1/queries").build();
            return toStates(jwtRestTemplate.exchange(authUser, HttpMethod.GET, getQueryUri, String.class));
        }
        
        @Override
        public QueryState getQuery(String queryId) {
            UriComponents getQueryUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort)
                            .path("/query-storage-test/v1/query/" + queryId).build();
            return toState(jwtRestTemplate.exchange(authUser, HttpMethod.GET, getQueryUri, String.class));
        }
        
        @Override
        public List<TaskDescription> getTasks(String queryId) {
            UriComponents getQueryUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort)
                            .path("/query-storage-test/v1/tasks/" + queryId).build();
            return toTaskDescriptions(jwtRestTemplate.exchange(authUser, HttpMethod.GET, getQueryUri, String.class));
            
        }
        
        private final ObjectMapper mapper = new ObjectMapper();
        
        private QueryState toState(ResponseEntity<String> responseEntity) {
            if (responseEntity.getBody() == null) {
                return null;
            }
            try {
                return mapper.readerFor(QueryState.class).readValue(responseEntity.getBody());
            } catch (IOException e) {
                throw new RuntimeException("Failed to decode value " + responseEntity.getBody(), e);
            }
        }
        
        private List<QueryState> toStates(ResponseEntity<String> responseEntity) {
            if (responseEntity.getBody() == null) {
                return null;
            }
            try {
                return mapper.readValue(responseEntity.getBody(), new TypeReference<List<QueryState>>() {});
            } catch (IOException e) {
                throw new RuntimeException("Failed to decode value " + responseEntity.getBody(), e);
            }
        }
        
        private List<TaskDescription> toTaskDescriptions(ResponseEntity<String> responseEntity) {
            if (responseEntity.getBody() == null) {
                return null;
            }
            try {
                return mapper.readValue(responseEntity.getBody(), new TypeReference<List<TaskDescription>>() {});
            } catch (IOException e) {
                throw new RuntimeException("Failed to decode value " + responseEntity.getBody(), e);
            }
        }
    }
}
