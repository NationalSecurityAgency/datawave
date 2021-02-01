package datawave.microservice.common.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.microservice.common.storage.config.QueryStorageConfig;
import datawave.microservice.common.storage.config.QueryStorageProperties;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryParametersImpl;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = QueryStorageServiceTest.QueryStorageServiceTestConfiguration.class)
@ActiveProfiles({"QueryStorageServiceTest", "sync-disabled"})
public class QueryStorageServiceTest {
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    private RestTemplateBuilder restTemplateBuilder;
    
    private JWTRestTemplate jwtRestTemplate;
    
    @Autowired
    private QueryStorageConfig.TaskNotificationSourceBinding taskNotificationSourceBinding;
    
    @Autowired
    private QueryStorageProperties queryStorageProperties;
    
    @Autowired
    private QueryStorageService storageService;
    
    @Autowired
    private QueryStorageStateService storageStateService;
    
    private SubjectIssuerDNPair DN;
    private String userDN = "userDn";
    private String query = "some query";
    private String authorizations = "AUTH1,AUTH2";
    
    @Before
    public void setup() {
        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        DN = SubjectIssuerDNPair.of(userDN, "issuerDn");
    }
    
    @Test
    public void testStoreQuery() throws ParseException {
        Query query = new QueryImpl();
        query.setQuery("foo == bar");
        query.setBeginDate(new SimpleDateFormat("yyyyMMdd").parse("20200101"));
        query.setEndDate(new SimpleDateFormat("yyyMMdd").parse("20210101"));
        QueryType type = new QueryType("default");
        UUID queryId = storageService.storeQuery(type, query);
        assertNotNull(queryId);
        
        QueryStorageStateService storageStateService = new TestQueryStateService("Administrator");
        QueryState state = storageStateService.getQuery(queryId.toString());
        assertQueryCreate(queryId, type, state);
        
        List<QueryState> queries = storageStateService.getRunningQueries();
        assertEquals(1, queries.size());
        assertQueryCreate(queryId, type, queries.get(0));
        
        queries = storageStateService.getRunningQueries(type.toString());
        assertEquals(1, queries.size());
        assertQueryCreate(queryId, type, queries.get(0));
        
        List<TaskDescription> tasks = storageStateService.getTasks(queryId.toString());
        assertEquals(1, tasks.size());
        UUID taskId = assertQueryCreate(queryId, query, tasks.get(0));
        
        QueryTask task = storageService.getTask(new QueryTaskNotification(taskId, queryId, type));
        assertQueryCreate(taskId, queryId, type, query, task);
    }
    
    private void assertQueryCreate(UUID queryId, QueryType type, QueryState state) {
        assertEquals(queryId, state.getQueryId());
        assertEquals(type, state.getQueryType());
        Map<QueryTask.QUERY_ACTION,Integer> counts = state.getTaskCounts();
        assertEquals(1, counts.size());
        assertTrue(counts.containsKey(QueryTask.QUERY_ACTION.CREATE));
        assertEquals(1, counts.get(QueryTask.QUERY_ACTION.CREATE).intValue());
    }
    
    private UUID assertQueryCreate(UUID queryId, Query query, TaskDescription task) throws ParseException {
        assertEquals(QueryTask.QUERY_ACTION.CREATE, task.getAction());
        String taskQuery = task.getParameters().get(QueryCheckpoint.INITIAL_QUERY_PROPERTY);
        assertTrue(taskQuery.contains(queryId.toString()));
        assertTrue(taskQuery.contains(query.getQuery()));
        assertTrue(taskQuery.contains(query.getBeginDate().toString()));
        assertTrue(taskQuery.contains(query.getEndDate().toString()));
        return task.getTaskId();
    }
    
    private void assertQueryCreate(UUID taskId, UUID queryId, QueryType type, Query query, QueryTask task) {
        assertEquals(taskId, task.getTaskId());
        assertEquals(QueryTask.QUERY_ACTION.CREATE, task.getAction());
        assertEquals(queryId, task.getQueryCheckpoint().getQueryKey().getQueryId());
        assertEquals(type, task.getQueryCheckpoint().getQueryKey().getType());
        assertEquals(1, task.getQueryCheckpoint().getProperties().size());
        assertEquals(query, task.getQueryCheckpoint().getProperties().get(QueryCheckpoint.INITIAL_QUERY_PROPERTY));
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
        public List<QueryState> getRunningQueries(String type) {
            UriComponents getQueryUri = UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort)
                            .path("/QueryStorage/v1/queries/" + type).build();
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
    
    @Configuration
    @Profile("QueryStorageServiceTest")
    @ComponentScan(basePackages = "datawave.microservice")
    public static class QueryStorageServiceTestConfiguration {}
    
}
