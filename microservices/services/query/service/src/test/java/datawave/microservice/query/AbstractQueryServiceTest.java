package datawave.microservice.query;

import static datawave.microservice.query.QueryParameters.QUERY_MAX_CONCURRENT_TASKS;
import static datawave.microservice.query.QueryParameters.QUERY_MAX_RESULTS_OVERRIDE;
import static datawave.microservice.query.QueryParameters.QUERY_PAGESIZE;
import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static datawave.webservice.common.audit.AuditParameters.AUDIT_ID;
import static org.springframework.test.web.client.ExpectedCount.never;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.anything;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.RequestMatcher;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.microservice.audit.AuditClient;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.QueryResultsPublisher;
import datawave.microservice.query.messaging.Result;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.QueryImplListResponse;
import datawave.webservice.result.QueryLogicResponse;
import datawave.webservice.result.VoidResponse;

public abstract class AbstractQueryServiceTest {
    protected static final String EXPECTED_AUDIT_URI = "http://localhost:11111/audit/v1/audit";
    protected static final String TEST_QUERY_STRING = "FIELD:SOME_VALUE";
    protected static final String TEST_QUERY_NAME = "The Greatest Query in the World - Tribute";
    protected static final String TEST_QUERY_AUTHORIZATIONS = "ALL";
    protected static final String TEST_QUERY_BEGIN = "20000101 000000.000";
    protected static final String TEST_QUERY_END = "20500101 000000.000";
    protected static final String TEST_VISIBILITY_MARKING = "ALL";
    protected static final long TEST_MAX_RESULTS_OVERRIDE = 369L;
    protected static final long TEST_PAGESIZE = 123L;
    protected static final long TEST_WAIT_TIME_MILLIS = TimeUnit.SECONDS.toMillis(60);
    
    @LocalServerPort
    protected int webServicePort;
    
    @Autowired
    protected RestTemplateBuilder restTemplateBuilder;
    
    protected JWTRestTemplate jwtRestTemplate;
    
    protected SubjectIssuerDNPair DN;
    protected String userDN = "userDn";
    
    protected SubjectIssuerDNPair altDN;
    protected String altUserDN = "altUserDN";
    
    @Autowired
    protected QueryStorageCache queryStorageCache;
    
    @Autowired
    protected QueryResultsManager queryQueueManager;
    
    @Autowired
    protected AuditClient auditClient;
    
    @Autowired
    protected QueryProperties queryProperties;
    
    @Autowired
    protected LinkedList<RemoteQueryRequestEvent> queryRequestEvents;
    
    protected List<String> auditIds;
    protected MockRestServiceServer mockServer;
    
    @BeforeEach
    public void setup() {
        auditIds = new ArrayList<>();
        
        jwtRestTemplate = restTemplateBuilder.build(JWTRestTemplate.class);
        jwtRestTemplate.setErrorHandler(new NoOpResponseErrorHandler());
        DN = SubjectIssuerDNPair.of(userDN, "issuerDn");
        altDN = SubjectIssuerDNPair.of(altUserDN, "issuerDN");
        
        RestTemplate auditorRestTemplate = (RestTemplate) new DirectFieldAccessor(auditClient).getPropertyValue("jwtRestTemplate");
        mockServer = MockRestServiceServer.createServer(auditorRestTemplate);
        
        queryRequestEvents.clear();
    }
    
    @AfterEach
    public void teardown() throws Exception {
        queryStorageCache.clear();
        queryRequestEvents.clear();
    }
    
    protected void publishEventsToQueue(String queryId, int numEvents, MultiValueMap<String,String> fieldValues, String visibility) throws Exception {
        QueryResultsPublisher publisher = queryQueueManager.createPublisher(queryId);
        for (int resultId = 0; resultId < numEvents; resultId++) {
            DefaultEvent event = new DefaultEvent();
            long currentTime = System.currentTimeMillis();
            List<DefaultField> fields = new ArrayList<>();
            for (Map.Entry<String,List<String>> entry : fieldValues.entrySet()) {
                for (String value : entry.getValue()) {
                    fields.add(new DefaultField(entry.getKey(), visibility, new HashMap<>(), currentTime, value));
                }
            }
            event.setFields(fields);
            publisher.publish(new Result(Integer.toString(resultId), event));
        }
    }
    
    protected String createQuery(DatawaveUserDetails authUser, MultiValueMap<String,String> map) {
        return newQuery(authUser, map, "create");
    }
    
    protected String defineQuery(DatawaveUserDetails authUser, MultiValueMap<String,String> map) {
        return newQuery(authUser, map, "define");
    }
    
    protected String newQuery(DatawaveUserDetails authUser, MultiValueMap<String,String> map, String createOrDefine) {
        UriComponents uri = createUri("EventQuery/" + createOrDefine);
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        ResponseEntity<GenericResponse> resp = jwtRestTemplate.exchange(requestEntity, GenericResponse.class);
        
        return (String) resp.getBody().getResult();
    }
    
    protected Future<ResponseEntity<DefaultEventQueryResponse>> nextQuery(DatawaveUserDetails authUser, String queryId) {
        UriComponents uri = createUri(queryId + "/next");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.GET, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, DefaultEventQueryResponse.class));
    }
    
    protected Future<ResponseEntity<VoidResponse>> adminCloseQuery(DatawaveUserDetails authUser, String queryId) {
        return stopQuery(authUser, queryId, "adminClose");
    }
    
    protected Future<ResponseEntity<VoidResponse>> closeQuery(DatawaveUserDetails authUser, String queryId) {
        return stopQuery(authUser, queryId, "close");
    }
    
    protected Future<ResponseEntity<VoidResponse>> adminCancelQuery(DatawaveUserDetails authUser, String queryId) {
        return stopQuery(authUser, queryId, "adminCancel");
    }
    
    protected Future<ResponseEntity<VoidResponse>> cancelQuery(DatawaveUserDetails authUser, String queryId) {
        return stopQuery(authUser, queryId, "cancel");
    }
    
    protected Future<ResponseEntity<VoidResponse>> stopQuery(DatawaveUserDetails authUser, String queryId, String closeOrCancel) {
        UriComponents uri = createUri(queryId + "/" + closeOrCancel);
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.PUT, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
    }
    
    protected Future<ResponseEntity<VoidResponse>> adminCloseAllQueries(DatawaveUserDetails authUser) {
        UriComponents uri = createUri("/adminCloseAll");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.PUT, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
    }
    
    protected Future<ResponseEntity<VoidResponse>> adminCancelAllQueries(DatawaveUserDetails authUser) {
        UriComponents uri = createUri("/adminCancelAll");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.PUT, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
    }
    
    protected Future<ResponseEntity<GenericResponse>> resetQuery(DatawaveUserDetails authUser, String queryId) {
        UriComponents uri = createUri(queryId + "/reset");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.PUT, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, GenericResponse.class));
    }
    
    protected Future<ResponseEntity<VoidResponse>> removeQuery(DatawaveUserDetails authUser, String queryId) {
        UriComponents uri = createUri(queryId + "/remove");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.DELETE, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
    }
    
    protected Future<ResponseEntity<VoidResponse>> adminRemoveQuery(DatawaveUserDetails authUser, String queryId) {
        UriComponents uri = createUri(queryId + "/adminRemove");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.DELETE, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
    }
    
    protected Future<ResponseEntity<VoidResponse>> adminRemoveAllQueries(DatawaveUserDetails authUser) {
        UriComponents uri = createUri("/adminRemoveAll");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.DELETE, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
    }
    
    protected Future<ResponseEntity<GenericResponse>> updateQuery(DatawaveUserDetails authUser, String queryId, MultiValueMap<String,String> map) {
        UriComponents uri = createUri(queryId + "/update");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.PUT, uri);
        
        // make the update call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, GenericResponse.class));
    }
    
    protected Future<ResponseEntity<GenericResponse>> duplicateQuery(DatawaveUserDetails authUser, String queryId, MultiValueMap<String,String> map) {
        UriComponents uri = createUri(queryId + "/duplicate");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        // make the update call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, GenericResponse.class));
    }
    
    protected Future<ResponseEntity<QueryImplListResponse>> listQueries(DatawaveUserDetails authUser, String queryId, String queryName) {
        UriComponentsBuilder uriBuilder = uriBuilder("/list");
        if (queryId != null) {
            uriBuilder.queryParam("queryId", queryId);
        }
        if (queryName != null) {
            uriBuilder.queryParam("queryName", queryName);
        }
        UriComponents uri = uriBuilder.build();
        
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.GET, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, QueryImplListResponse.class));
    }
    
    protected Future<ResponseEntity<QueryImplListResponse>> getQuery(DatawaveUserDetails authUser, String queryId) {
        UriComponents uri = createUri(queryId);
        
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.GET, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, QueryImplListResponse.class));
    }
    
    protected Future<ResponseEntity<QueryLogicResponse>> listQueryLogic(DatawaveUserDetails authUser) {
        UriComponents uri = createUri("/listQueryLogic");
        
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.GET, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, QueryLogicResponse.class));
    }
    
    protected Future<ResponseEntity<QueryImplListResponse>> adminListQueries(DatawaveUserDetails authUser, String queryId, String user, String queryName) {
        UriComponentsBuilder uriBuilder = uriBuilder("/adminList");
        if (queryId != null) {
            uriBuilder.queryParam("queryId", queryId);
        }
        if (queryName != null) {
            uriBuilder.queryParam("queryName", queryName);
        }
        if (user != null) {
            uriBuilder.queryParam("user", user);
        }
        UriComponents uri = uriBuilder.build();
        
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.GET, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, QueryImplListResponse.class));
    }
    
    protected DatawaveUserDetails createUserDetails() {
        return createUserDetails(null, null);
    }
    
    protected DatawaveUserDetails createUserDetails(Collection<String> roles, Collection<String> auths) {
        Collection<String> userRoles = roles != null ? roles : Collections.singleton("AuthorizedUser");
        Collection<String> userAuths = auths != null ? auths : Collections.singleton("ALL");
        DatawaveUser datawaveUser = new DatawaveUser(DN, USER, userAuths, userRoles, null, System.currentTimeMillis());
        return new DatawaveUserDetails(Collections.singleton(datawaveUser), datawaveUser.getCreationTime());
    }
    
    protected DatawaveUserDetails createAltUserDetails() {
        return createAltUserDetails(null, null);
    }
    
    protected DatawaveUserDetails createAltUserDetails(Collection<String> roles, Collection<String> auths) {
        Collection<String> userRoles = roles != null ? roles : Collections.singleton("AuthorizedUser");
        Collection<String> userAuths = auths != null ? auths : Collections.singleton("ALL");
        DatawaveUser datawaveUser = new DatawaveUser(altDN, USER, userAuths, userRoles, null, System.currentTimeMillis());
        return new DatawaveUserDetails(Collections.singleton(datawaveUser), datawaveUser.getCreationTime());
    }
    
    protected UriComponentsBuilder uriBuilder(String path) {
        return UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/query/v1/query/" + path);
    }
    
    protected UriComponents createUri(String path) {
        return uriBuilder(path).build();
    }
    
    protected MultiValueMap<String,String> createParams() {
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.set(DefaultQueryParameters.QUERY_STRING, TEST_QUERY_STRING);
        map.set(DefaultQueryParameters.QUERY_NAME, TEST_QUERY_NAME);
        map.set(DefaultQueryParameters.QUERY_AUTHORIZATIONS, TEST_QUERY_AUTHORIZATIONS);
        map.set(DefaultQueryParameters.QUERY_BEGIN, TEST_QUERY_BEGIN);
        map.set(DefaultQueryParameters.QUERY_END, TEST_QUERY_END);
        map.set(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, TEST_VISIBILITY_MARKING);
        map.set(QUERY_MAX_CONCURRENT_TASKS, Integer.toString(1));
        map.set(QUERY_MAX_RESULTS_OVERRIDE, Long.toString(TEST_MAX_RESULTS_OVERRIDE));
        map.set(QUERY_PAGESIZE, Long.toString(TEST_PAGESIZE));
        return map;
    }
    
    protected void assertDefaultEvent(List<String> fields, List<String> values, DefaultEvent event) {
        Assertions.assertEquals(fields, event.getFields().stream().map(DefaultField::getName).collect(Collectors.toList()));
        Assertions.assertEquals(values, event.getFields().stream().map(DefaultField::getValueString).collect(Collectors.toList()));
    }
    
    protected void assertQueryResponse(String queryId, String logicName, long pageNumber, boolean partialResults, long operationTimeInMS, int numFields,
                    List<String> fieldNames, int numEvents, DefaultEventQueryResponse queryResponse) {
        Assertions.assertEquals(queryId, queryResponse.getQueryId());
        Assertions.assertEquals(logicName, queryResponse.getLogicName());
        Assertions.assertEquals(pageNumber, queryResponse.getPageNumber());
        Assertions.assertEquals(partialResults, queryResponse.isPartialResults());
        Assertions.assertEquals(operationTimeInMS, queryResponse.getOperationTimeMS());
        Assertions.assertEquals(numFields, queryResponse.getFields().size());
        Assertions.assertEquals(fieldNames, queryResponse.getFields());
        Assertions.assertEquals(numEvents, queryResponse.getEvents().size());
    }
    
    protected void assertQueryRequestEvent(String destination, QueryRequest.Method method, String queryId, RemoteQueryRequestEvent queryRequestEvent) {
        Assertions.assertEquals(destination, queryRequestEvent.getDestinationService());
        Assertions.assertEquals(queryId, queryRequestEvent.getRequest().getQueryId());
        Assertions.assertEquals(method, queryRequestEvent.getRequest().getMethod());
    }
    
    protected void assertQueryStatus(QueryStatus.QUERY_STATE queryState, long numResultsReturned, long numResultsGenerated, long activeNextCalls,
                    long lastPageNumber, long lastCallTimeMillis, QueryStatus queryStatus) {
        Assertions.assertEquals(queryState, queryStatus.getQueryState());
        Assertions.assertEquals(numResultsReturned, queryStatus.getNumResultsReturned());
        Assertions.assertEquals(numResultsGenerated, queryStatus.getNumResultsGenerated());
        Assertions.assertEquals(activeNextCalls, queryStatus.getActiveNextCalls());
        Assertions.assertEquals(lastPageNumber, queryStatus.getLastPageNumber());
        Assertions.assertTrue(queryStatus.getLastUsedMillis() > lastCallTimeMillis);
        Assertions.assertTrue(queryStatus.getLastUpdatedMillis() > lastCallTimeMillis);
    }
    
    protected void assertQuery(String queryString, String queryName, String authorizations, String begin, String end, String visibility, Query query)
                    throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(DefaultQueryParameters.formatPattern);
        Assertions.assertEquals(queryString, query.getQuery());
        Assertions.assertEquals(queryName, query.getQueryName());
        Assertions.assertEquals(authorizations, query.getQueryAuthorizations());
        Assertions.assertEquals(sdf.parse(begin), query.getBeginDate());
        Assertions.assertEquals(sdf.parse(end), query.getEndDate());
        Assertions.assertEquals(visibility, query.getColumnVisibility());
    }
    
    protected void assertTasksCreated(String queryId) throws IOException {
        // verify that the query task states were created
        TaskStates taskStates = queryStorageCache.getTaskStates(queryId);
        Assertions.assertNotNull(taskStates);
        
        // verify that a query task was created
        List<TaskKey> taskKeys = queryStorageCache.getTasks(queryId);
        Assertions.assertFalse(taskKeys.isEmpty());
    }
    
    protected void assertTasksNotCreated(String queryId) throws IOException {
        // verify that the query task states were not created
        TaskStates taskStates = queryStorageCache.getTaskStates(queryId);
        Assertions.assertNull(taskStates);
        
        // verify that a query task was not created
        List<TaskKey> taskKeys = queryStorageCache.getTasks(queryId);
        Assertions.assertTrue(taskKeys.isEmpty());
    }
    
    public RequestMatcher auditIdGrabber() {
        return request -> {
            List<NameValuePair> params = URLEncodedUtils.parse(request.getBody().toString(), Charset.defaultCharset());
            params.stream().filter(p -> p.getName().equals(AUDIT_ID)).forEach(p -> auditIds.add(p.getValue()));
        };
    }
    
    protected void auditIgnoreSetup() {
        mockServer.expect(anything()).andRespond(withSuccess());
    }
    
    protected void auditSentSetup() {
        mockServer.expect(requestTo(EXPECTED_AUDIT_URI)).andExpect(auditIdGrabber()).andRespond(withSuccess());
    }
    
    protected void auditNotSentSetup() {
        mockServer.expect(never(), requestTo(EXPECTED_AUDIT_URI)).andExpect(auditIdGrabber()).andRespond(withSuccess());
    }
    
    protected void assertAuditSent(String queryId) {
        mockServer.verify();
        Assertions.assertEquals(1, auditIds.size());
        if (queryId != null) {
            Assertions.assertEquals(queryId, auditIds.get(0));
        }
    }
    
    protected void assertAuditNotSent() {
        mockServer.verify();
        Assertions.assertEquals(0, auditIds.size());
    }
    
    protected void assertQueryException(String message, String cause, String code, QueryExceptionType queryException) {
        Assertions.assertEquals(message, queryException.getMessage());
        Assertions.assertEquals(cause, queryException.getCause());
        Assertions.assertEquals(code, queryException.getCode());
    }
    
    protected BaseResponse assertBaseResponse(boolean hasResults, HttpStatus.Series series, ResponseEntity response) {
        Assertions.assertEquals(series, response.getStatusCode().series());
        Assertions.assertNotNull(response);
        BaseResponse baseResponse = (BaseResponse) response.getBody();
        Assertions.assertNotNull(baseResponse);
        Assertions.assertEquals(hasResults, baseResponse.getHasResults());
        return baseResponse;
    }
    
    @SuppressWarnings("unchecked")
    protected GenericResponse<String> assertGenericResponse(boolean hasResults, HttpStatus.Series series, ResponseEntity<GenericResponse> response) {
        Assertions.assertEquals(series, response.getStatusCode().series());
        Assertions.assertNotNull(response);
        GenericResponse<String> genericResponse = (GenericResponse<String>) response.getBody();
        Assertions.assertNotNull(genericResponse);
        Assertions.assertEquals(hasResults, genericResponse.getHasResults());
        return genericResponse;
    }
    
    protected static class NoOpResponseErrorHandler extends DefaultResponseErrorHandler {
        @Override
        public void handleError(ClientHttpResponse response) throws IOException {
            // do nothing
        }
    }
    
    @Configuration
    @Profile("QueryServiceTest")
    public static class QueryServiceTestConfiguration {
        @Bean
        public Module queryImplDeserializer(@Lazy ObjectMapper objectMapper) {
            SimpleModule module = new SimpleModule();
            module.addDeserializer(Query.class, new JsonDeserializer<>() {
                @Override
                public Query deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException {
                    return objectMapper.readValue(jsonParser, QueryImpl.class);
                }
            });
            return module;
        }
        
        @Bean
        public HazelcastInstance hazelcastInstance() {
            Config config = new Config();
            config.setClusterName(UUID.randomUUID().toString());
            return Hazelcast.newHazelcastInstance(config);
        }
        
        @Bean
        public LinkedList<RemoteQueryRequestEvent> queryRequestEvents() {
            return new LinkedList<>();
        }
        
        @Bean
        @Primary
        public ApplicationEventPublisher eventPublisher(@Lazy QueryManagementService queryManagementService, ServiceMatcher serviceMatcher) {
            return new ApplicationEventPublisher() {
                @Override
                public void publishEvent(ApplicationEvent event) {
                    saveEvent(event);
                    processEvent(event);
                }
                
                @Override
                public void publishEvent(Object event) {
                    saveEvent(event);
                    processEvent(event);
                }
                
                private void saveEvent(Object event) {
                    if (event instanceof RemoteQueryRequestEvent) {
                        queryRequestEvents().push(((RemoteQueryRequestEvent) event));
                    }
                }
                
                private void processEvent(Object event) {
                    if (event instanceof RemoteQueryRequestEvent) {
                        RemoteQueryRequestEvent queryEvent = (RemoteQueryRequestEvent) event;
                        boolean isSelfRequest = serviceMatcher.isFromSelf(queryEvent);
                        if (!isSelfRequest) {
                            queryManagementService.handleRemoteRequest(queryEvent.getRequest(), queryEvent.getOriginService(),
                                            queryEvent.getDestinationService());
                        }
                    }
                }
            };
        }
    }
}
