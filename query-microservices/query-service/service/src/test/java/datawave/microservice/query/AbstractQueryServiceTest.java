package datawave.microservice.query;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.microservice.audit.AuditClient;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.messaging.Result;
import datawave.microservice.query.messaging.TestQueryResultsManager;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.QueryImplListResponse;
import datawave.webservice.result.QueryLogicResponse;
import datawave.webservice.result.VoidResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.junit.Assert;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
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

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static datawave.microservice.query.QueryParameters.QUERY_MAX_CONCURRENT_TASKS;
import static datawave.microservice.query.QueryParameters.QUERY_MAX_RESULTS_OVERRIDE;
import static datawave.microservice.query.QueryParameters.QUERY_PAGESIZE;
import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static datawave.webservice.common.audit.AuditParameters.AUDIT_ID;
import static org.springframework.test.web.client.ExpectedCount.never;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.anything;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

public abstract class AbstractQueryServiceTest {
    protected static final String EXPECTED_AUDIT_URI = "http://localhost:11111/audit/v1/audit";
    protected static final String TEST_QUERY_STRING = "FIELD:SOME_VALUE";
    protected static final String TEST_QUERY_NAME = "The Greatest Query in the World - Tribute";
    protected static final String TEST_QUERY_AUTHORIZATIONS = "ALL";
    protected static final String TEST_QUERY_BEGIN = "20000101 000000.000";
    protected static final String TEST_QUERY_END = "20500101 000000.000";
    protected static final String TEST_VISIBILITY_MARKING = "ALL";
    
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
    protected TestQueryResultsManager queryQueueManager;
    
    @Autowired
    protected AuditClient auditClient;
    
    @Autowired
    protected QueryProperties queryProperties;
    
    @Autowired
    protected LinkedList<RemoteQueryRequestEvent> queryRequestEvents;
    
    protected List<String> auditIds;
    protected MockRestServiceServer mockServer;
    
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
    
    public void teardown() throws Exception {
        queryStorageCache.clear();
        queryRequestEvents.clear();
        queryQueueManager.clear();
    }
    
    protected void publishEventsToQueue(String queryId, int numEvents, MultiValueMap<String,String> fieldValues, String visibility) throws Exception {
        for (int resultId = 0; resultId < numEvents; resultId++) {
            DefaultEvent event = new DefaultEvent();
            long currentTime = System.currentTimeMillis();
            List<DefaultField> fields = new ArrayList<>();
            for (Map.Entry<String,List<String>> entry : fieldValues.entrySet()) {
                for (String value : entry.getValue()) {
                    fields.add(new DefaultField(entry.getKey(), visibility, currentTime, value));
                }
            }
            event.setFields(fields);
            queryQueueManager.createPublisher(queryId).publish(new Result(Integer.toString(resultId), event));
        }
    }
    
    protected String createQuery(ProxiedUserDetails authUser, MultiValueMap<String,String> map) {
        return newQuery(authUser, map, "create");
    }
    
    protected String defineQuery(ProxiedUserDetails authUser, MultiValueMap<String,String> map) {
        return newQuery(authUser, map, "define");
    }
    
    protected String newQuery(ProxiedUserDetails authUser, MultiValueMap<String,String> map, String createOrDefine) {
        UriComponents uri = createUri("EventQuery/" + createOrDefine);
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        ResponseEntity<GenericResponse> resp = jwtRestTemplate.exchange(requestEntity, GenericResponse.class);
        
        return (String) resp.getBody().getResult();
    }
    
    protected Future<ResponseEntity<BaseResponse>> nextQuery(ProxiedUserDetails authUser, String queryId) {
        UriComponents uri = createUri(queryId + "/next");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.GET, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, BaseResponse.class));
    }
    
    protected Future<ResponseEntity<VoidResponse>> adminCloseQuery(ProxiedUserDetails authUser, String queryId) {
        return stopQuery(authUser, queryId, "adminClose");
    }
    
    protected Future<ResponseEntity<VoidResponse>> closeQuery(ProxiedUserDetails authUser, String queryId) {
        return stopQuery(authUser, queryId, "close");
    }
    
    protected Future<ResponseEntity<VoidResponse>> adminCancelQuery(ProxiedUserDetails authUser, String queryId) {
        return stopQuery(authUser, queryId, "adminCancel");
    }
    
    protected Future<ResponseEntity<VoidResponse>> cancelQuery(ProxiedUserDetails authUser, String queryId) {
        return stopQuery(authUser, queryId, "cancel");
    }
    
    protected Future<ResponseEntity<VoidResponse>> stopQuery(ProxiedUserDetails authUser, String queryId, String closeOrCancel) {
        UriComponents uri = createUri(queryId + "/" + closeOrCancel);
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.PUT, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
    }
    
    protected Future<ResponseEntity<VoidResponse>> adminCloseAllQueries(ProxiedUserDetails authUser) {
        UriComponents uri = createUri("/adminCloseAll");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.PUT, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
    }
    
    protected Future<ResponseEntity<VoidResponse>> adminCancelAllQueries(ProxiedUserDetails authUser) {
        UriComponents uri = createUri("/adminCancelAll");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.PUT, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
    }
    
    protected Future<ResponseEntity<GenericResponse>> resetQuery(ProxiedUserDetails authUser, String queryId) {
        UriComponents uri = createUri(queryId + "/reset");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.PUT, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, GenericResponse.class));
    }
    
    protected Future<ResponseEntity<VoidResponse>> removeQuery(ProxiedUserDetails authUser, String queryId) {
        UriComponents uri = createUri(queryId + "/remove");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.DELETE, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
    }
    
    protected Future<ResponseEntity<VoidResponse>> adminRemoveQuery(ProxiedUserDetails authUser, String queryId) {
        UriComponents uri = createUri(queryId + "/adminRemove");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.DELETE, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
    }
    
    protected Future<ResponseEntity<VoidResponse>> adminRemoveAllQueries(ProxiedUserDetails authUser) {
        UriComponents uri = createUri("/adminRemoveAll");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.DELETE, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
    }
    
    protected Future<ResponseEntity<GenericResponse>> updateQuery(ProxiedUserDetails authUser, String queryId, MultiValueMap<String,String> map) {
        UriComponents uri = createUri(queryId + "/update");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.PUT, uri);
        
        // make the update call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, GenericResponse.class));
    }
    
    protected Future<ResponseEntity<GenericResponse>> duplicateQuery(ProxiedUserDetails authUser, String queryId, MultiValueMap<String,String> map) {
        UriComponents uri = createUri(queryId + "/duplicate");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        // make the update call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, GenericResponse.class));
    }
    
    protected Future<ResponseEntity<QueryImplListResponse>> listQueries(ProxiedUserDetails authUser, String queryId, String queryName) {
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
    
    protected Future<ResponseEntity<QueryImplListResponse>> getQuery(ProxiedUserDetails authUser, String queryId) {
        UriComponents uri = createUri(queryId);
        
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.GET, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, QueryImplListResponse.class));
    }
    
    protected Future<ResponseEntity<QueryLogicResponse>> listQueryLogic(ProxiedUserDetails authUser) {
        UriComponents uri = createUri("/listQueryLogic");
        
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.GET, uri);
        
        // make the next call asynchronously
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, QueryLogicResponse.class));
    }
    
    protected Future<ResponseEntity<QueryImplListResponse>> adminListQueries(ProxiedUserDetails authUser, String queryId, String user, String queryName) {
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
    
    protected ProxiedUserDetails createUserDetails() {
        return createUserDetails(null, null);
    }
    
    protected ProxiedUserDetails createUserDetails(Collection<String> roles, Collection<String> auths) {
        Collection<String> userRoles = roles != null ? roles : Collections.singleton("AuthorizedUser");
        Collection<String> userAuths = auths != null ? auths : Collections.singleton("ALL");
        DatawaveUser datawaveUser = new DatawaveUser(DN, USER, userAuths, userRoles, null, System.currentTimeMillis());
        return new ProxiedUserDetails(Collections.singleton(datawaveUser), datawaveUser.getCreationTime());
    }
    
    protected ProxiedUserDetails createAltUserDetails() {
        return createAltUserDetails(null, null);
    }
    
    protected ProxiedUserDetails createAltUserDetails(Collection<String> roles, Collection<String> auths) {
        Collection<String> userRoles = roles != null ? roles : Collections.singleton("AuthorizedUser");
        Collection<String> userAuths = auths != null ? auths : Collections.singleton("ALL");
        DatawaveUser datawaveUser = new DatawaveUser(altDN, USER, userAuths, userRoles, null, System.currentTimeMillis());
        return new ProxiedUserDetails(Collections.singleton(datawaveUser), datawaveUser.getCreationTime());
    }
    
    protected UriComponentsBuilder uriBuilder(String path) {
        return UriComponentsBuilder.newInstance().scheme("https").host("localhost").port(webServicePort).path("/query/v1/" + path);
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
        map.set(QUERY_MAX_RESULTS_OVERRIDE, Long.toString(369));
        map.set(QUERY_PAGESIZE, Long.toString(123));
        return map;
    }
    
    protected void assertDefaultEvent(List<String> fields, List<String> values, DefaultEvent event) {
        Assert.assertEquals(fields, event.getFields().stream().map(DefaultField::getName).collect(Collectors.toList()));
        Assert.assertEquals(values, event.getFields().stream().map(DefaultField::getValueString).collect(Collectors.toList()));
    }
    
    protected void assertQueryResponse(String queryId, String logicName, long pageNumber, boolean partialResults, long operationTimeInMS, int numFields,
                    List<String> fieldNames, int numEvents, DefaultEventQueryResponse queryResponse) {
        Assert.assertEquals(queryId, queryResponse.getQueryId());
        Assert.assertEquals(logicName, queryResponse.getLogicName());
        Assert.assertEquals(pageNumber, queryResponse.getPageNumber());
        Assert.assertEquals(partialResults, queryResponse.isPartialResults());
        Assert.assertEquals(operationTimeInMS, queryResponse.getOperationTimeMS());
        Assert.assertEquals(numFields, queryResponse.getFields().size());
        Assert.assertEquals(fieldNames, queryResponse.getFields());
        Assert.assertEquals(numEvents, queryResponse.getEvents().size());
    }
    
    protected void assertQueryRequestEvent(String destination, QueryRequest.Method method, String queryId, RemoteQueryRequestEvent queryRequestEvent) {
        Assert.assertEquals(destination, queryRequestEvent.getDestinationService());
        Assert.assertEquals(queryId, queryRequestEvent.getRequest().getQueryId());
        Assert.assertEquals(method, queryRequestEvent.getRequest().getMethod());
    }
    
    protected void assertQueryStatus(QueryStatus.QUERY_STATE queryState, long numResultsReturned, long numResultsGenerated, long activeNextCalls,
                    long lastPageNumber, long lastCallTimeMillis, QueryStatus queryStatus) {
        Assert.assertEquals(queryState, queryStatus.getQueryState());
        Assert.assertEquals(numResultsReturned, queryStatus.getNumResultsReturned());
        Assert.assertEquals(numResultsGenerated, queryStatus.getNumResultsGenerated());
        Assert.assertEquals(activeNextCalls, queryStatus.getActiveNextCalls());
        Assert.assertEquals(lastPageNumber, queryStatus.getLastPageNumber());
        Assert.assertTrue(queryStatus.getLastUsedMillis() > lastCallTimeMillis);
        Assert.assertTrue(queryStatus.getLastUpdatedMillis() > lastCallTimeMillis);
    }
    
    protected void assertQuery(String queryString, String queryName, String authorizations, String begin, String end, String visibility, Query query)
                    throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(DefaultQueryParameters.formatPattern);
        Assert.assertEquals(queryString, query.getQuery());
        Assert.assertEquals(queryName, query.getQueryName());
        Assert.assertEquals(authorizations, query.getQueryAuthorizations());
        Assert.assertEquals(sdf.parse(begin), query.getBeginDate());
        Assert.assertEquals(sdf.parse(end), query.getEndDate());
        Assert.assertEquals(visibility, query.getColumnVisibility());
    }
    
    protected void assertTasksCreated(String queryId) throws IOException {
        // verify that the query task states were created
        TaskStates taskStates = queryStorageCache.getTaskStates(queryId);
        Assert.assertNotNull(taskStates);
        
        // verify that a query task was created
        List<TaskKey> taskKeys = queryStorageCache.getTasks(queryId);
        Assert.assertFalse(taskKeys.isEmpty());
    }
    
    protected void assertTasksNotCreated(String queryId) throws IOException {
        // verify that the query task states were not created
        TaskStates taskStates = queryStorageCache.getTaskStates(queryId);
        Assert.assertNull(taskStates);
        
        // verify that a query task was not created
        List<TaskKey> taskKeys = queryStorageCache.getTasks(queryId);
        Assert.assertTrue(taskKeys.isEmpty());
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
        Assert.assertEquals(1, auditIds.size());
        Assert.assertEquals(queryId, auditIds.get(0));
    }
    
    protected void assertAuditNotSent() {
        mockServer.verify();
        Assert.assertEquals(0, auditIds.size());
    }
    
    protected void assertQueryException(String message, String cause, String code, QueryExceptionType queryException) {
        Assert.assertEquals(message, queryException.getMessage());
        Assert.assertEquals(cause, queryException.getCause());
        Assert.assertEquals(code, queryException.getCode());
    }
    
    protected BaseResponse assertBaseResponse(boolean hasResults, HttpStatus.Series series, ResponseEntity<BaseResponse> response) {
        Assert.assertEquals(series, response.getStatusCode().series());
        Assert.assertNotNull(response);
        BaseResponse baseResponse = response.getBody();
        Assert.assertNotNull(baseResponse);
        Assert.assertEquals(hasResults, baseResponse.getHasResults());
        return baseResponse;
    }
    
    @SuppressWarnings("unchecked")
    protected GenericResponse<String> assertGenericResponse(boolean hasResults, HttpStatus.Series series, ResponseEntity<GenericResponse> response) {
        Assert.assertEquals(series, response.getStatusCode().series());
        Assert.assertNotNull(response);
        GenericResponse<String> genericResponse = (GenericResponse<String>) response.getBody();
        Assert.assertNotNull(genericResponse);
        Assert.assertEquals(hasResults, genericResponse.getHasResults());
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
    @ComponentScan(basePackages = "datawave.microservice")
    public static class QueryServiceTestConfiguration {
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
