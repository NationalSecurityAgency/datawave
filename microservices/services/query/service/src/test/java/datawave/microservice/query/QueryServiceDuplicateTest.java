package datawave.microservice.query;

import static datawave.microservice.query.QueryImpl.BEGIN_DATE;
import static datawave.microservice.query.QueryImpl.END_DATE;
import static datawave.microservice.query.QueryImpl.QUERY;
import static datawave.microservice.query.QueryParameters.QUERY_LOGIC_NAME;
import static datawave.webservice.common.audit.AuditParameters.QUERY_AUTHORIZATIONS;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;

import com.google.common.collect.Iterables;

import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryStarterDefaults", "QueryStarterOverrides", "QueryServiceTest", RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE})
@ContextConfiguration(classes = {QueryService.class})
public class QueryServiceDuplicateTest extends AbstractQueryServiceTest {
    
    @Test
    public void testDuplicateSuccess_duplicateOnDefined() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // define a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = defineQuery(authUser, createParams());
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        
        mockServer.reset();
        auditSentSetup();
        
        // duplicate the query
        Future<ResponseEntity<GenericResponse>> duplicateFuture = duplicateQuery(authUser, queryId, updateParams);
        
        // the response should come back right away
        ResponseEntity<GenericResponse> response = duplicateFuture.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        String dupeQueryId = (String) response.getBody().getResult();
        
        // make sure an audit message was sent
        assertAuditSent(dupeQueryId);
        
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.DEFINE,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                queryStatus);
        // @formatter:on
        
        QueryStatus dupeQueryStatus = queryStorageCache.getQueryStatus(dupeQueryId);
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CREATE,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                dupeQueryStatus);
        // @formatter:on
        
        // make sure the queries are identical
        Assertions.assertEquals(queryStatus.getQuery().getQuery(), dupeQueryStatus.getQuery().getQuery());
        Assertions.assertEquals(queryStatus.getQuery().getQueryAuthorizations(), dupeQueryStatus.getQuery().getQueryAuthorizations());
        Assertions.assertEquals(DefaultQueryParameters.formatDate(queryStatus.getQuery().getBeginDate()),
                        DefaultQueryParameters.formatDate(dupeQueryStatus.getQuery().getBeginDate()));
        Assertions.assertEquals(DefaultQueryParameters.formatDate(queryStatus.getQuery().getEndDate()),
                        DefaultQueryParameters.formatDate(dupeQueryStatus.getQuery().getEndDate()));
        Assertions.assertEquals(queryStatus.getQuery().getQueryLogicName(), dupeQueryStatus.getQuery().getQueryLogicName());
        Assertions.assertEquals(queryStatus.getQuery().getPagesize(), dupeQueryStatus.getQuery().getPagesize());
        
        // verify that no events were published
        Assertions.assertEquals(1, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                dupeQueryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testDuplicateSuccess_duplicateOnCreated() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = createQuery(authUser, createParams());
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        
        mockServer.reset();
        auditSentSetup();
        
        // duplicate the query
        Future<ResponseEntity<GenericResponse>> duplicateFuture = duplicateQuery(authUser, queryId, updateParams);
        
        // the response should come back right away
        ResponseEntity<GenericResponse> response = duplicateFuture.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        String dupeQueryId = (String) response.getBody().getResult();
        
        // make sure an audit message was sent
        assertAuditSent(dupeQueryId);
        
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CREATE,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                queryStatus);
        // @formatter:on
        
        QueryStatus dupeQueryStatus = queryStorageCache.getQueryStatus(dupeQueryId);
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CREATE,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                dupeQueryStatus);
        // @formatter:on
        
        // make sure the queries are identical
        Assertions.assertEquals(queryStatus.getQuery().getQuery(), dupeQueryStatus.getQuery().getQuery());
        Assertions.assertEquals(queryStatus.getQuery().getQueryAuthorizations(), dupeQueryStatus.getQuery().getQueryAuthorizations());
        Assertions.assertEquals(DefaultQueryParameters.formatDate(queryStatus.getQuery().getBeginDate()),
                        DefaultQueryParameters.formatDate(dupeQueryStatus.getQuery().getBeginDate()));
        Assertions.assertEquals(DefaultQueryParameters.formatDate(queryStatus.getQuery().getEndDate()),
                        DefaultQueryParameters.formatDate(dupeQueryStatus.getQuery().getEndDate()));
        Assertions.assertEquals(queryStatus.getQuery().getQueryLogicName(), dupeQueryStatus.getQuery().getQueryLogicName());
        Assertions.assertEquals(queryStatus.getQuery().getPagesize(), dupeQueryStatus.getQuery().getPagesize());
        
        // verify that no events were published
        Assertions.assertEquals(2, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                dupeQueryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testDuplicateSuccess_duplicateOnCanceled() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = createQuery(authUser, createParams());
        
        // cancel the query
        Future<ResponseEntity<VoidResponse>> cancelFuture = cancelQuery(authUser, queryId);
        
        // this should return immediately
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assertions.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        
        mockServer.reset();
        auditSentSetup();
        
        // duplicate the query
        Future<ResponseEntity<GenericResponse>> duplicateFuture = duplicateQuery(authUser, queryId, updateParams);
        
        // the response should come back right away
        ResponseEntity<GenericResponse> response = duplicateFuture.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        String dupeQueryId = (String) response.getBody().getResult();
        
        // make sure an audit message was sent
        assertAuditSent(dupeQueryId);
        
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CANCEL,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                queryStatus);
        // @formatter:on
        
        QueryStatus dupeQueryStatus = queryStorageCache.getQueryStatus(dupeQueryId);
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CREATE,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                dupeQueryStatus);
        // @formatter:on
        
        // make sure the queries are identical
        Assertions.assertEquals(queryStatus.getQuery().getQuery(), dupeQueryStatus.getQuery().getQuery());
        Assertions.assertEquals(queryStatus.getQuery().getQueryAuthorizations(), dupeQueryStatus.getQuery().getQueryAuthorizations());
        Assertions.assertEquals(DefaultQueryParameters.formatDate(queryStatus.getQuery().getBeginDate()),
                        DefaultQueryParameters.formatDate(dupeQueryStatus.getQuery().getBeginDate()));
        Assertions.assertEquals(DefaultQueryParameters.formatDate(queryStatus.getQuery().getEndDate()),
                        DefaultQueryParameters.formatDate(dupeQueryStatus.getQuery().getEndDate()));
        Assertions.assertEquals(queryStatus.getQuery().getQueryLogicName(), dupeQueryStatus.getQuery().getQueryLogicName());
        Assertions.assertEquals(queryStatus.getQuery().getPagesize(), dupeQueryStatus.getQuery().getPagesize());
        
        // verify that no events were published
        Assertions.assertEquals(4, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "query:**",
                QueryRequest.Method.CANCEL,
                queryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CANCEL,
                queryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                dupeQueryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testDuplicateSuccess_duplicateOnClosed() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = createQuery(authUser, createParams());
        
        // close the query
        Future<ResponseEntity<VoidResponse>> closeFuture = closeQuery(authUser, queryId);
        
        // this should return immediately
        ResponseEntity<VoidResponse> closeResponse = closeFuture.get();
        
        Assertions.assertEquals(200, closeResponse.getStatusCodeValue());
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        
        mockServer.reset();
        auditSentSetup();
        
        // duplicate the query
        Future<ResponseEntity<GenericResponse>> duplicateFuture = duplicateQuery(authUser, queryId, updateParams);
        
        // the response should come back right away
        ResponseEntity<GenericResponse> response = duplicateFuture.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        String dupeQueryId = (String) response.getBody().getResult();
        
        // make sure an audit message was sent
        assertAuditSent(dupeQueryId);
        
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CLOSE,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                queryStatus);
        // @formatter:on
        
        QueryStatus dupeQueryStatus = queryStorageCache.getQueryStatus(dupeQueryId);
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CREATE,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                dupeQueryStatus);
        // @formatter:on
        
        // make sure the queries are identical
        Assertions.assertEquals(queryStatus.getQuery().getQuery(), dupeQueryStatus.getQuery().getQuery());
        Assertions.assertEquals(queryStatus.getQuery().getQueryAuthorizations(), dupeQueryStatus.getQuery().getQueryAuthorizations());
        Assertions.assertEquals(DefaultQueryParameters.formatDate(queryStatus.getQuery().getBeginDate()),
                        DefaultQueryParameters.formatDate(dupeQueryStatus.getQuery().getBeginDate()));
        Assertions.assertEquals(DefaultQueryParameters.formatDate(queryStatus.getQuery().getEndDate()),
                        DefaultQueryParameters.formatDate(dupeQueryStatus.getQuery().getEndDate()));
        Assertions.assertEquals(queryStatus.getQuery().getQueryLogicName(), dupeQueryStatus.getQuery().getQueryLogicName());
        Assertions.assertEquals(queryStatus.getQuery().getPagesize(), dupeQueryStatus.getQuery().getPagesize());
        
        // verify that no events were published
        Assertions.assertEquals(3, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CLOSE,
                queryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                dupeQueryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testDuplicateSuccess_update() throws Exception {
        DatawaveUserDetails authUser = createUserDetails(null, Arrays.asList("ALL", "NONE"));
        
        // define a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = defineQuery(authUser, createParams());
        
        String newQuery = "SOME_OTHER_FIELD:SOME_OTHER_VALUE";
        String newAuths = "ALL,NONE";
        String newBegin = "20100101 000000.000";
        String newEnd = "20600101 000000.000";
        String newLogic = "AltEventQuery";
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        updateParams.set(QUERY, newQuery);
        updateParams.set(QUERY_AUTHORIZATIONS, newAuths);
        updateParams.set(BEGIN_DATE, newBegin);
        updateParams.set(END_DATE, newEnd);
        updateParams.set(QUERY_LOGIC_NAME, newLogic);
        
        mockServer.reset();
        auditSentSetup();
        
        // duplicate the query
        Future<ResponseEntity<GenericResponse>> duplicateFuture = duplicateQuery(authUser, queryId, updateParams);
        
        // the response should come back right away
        ResponseEntity<GenericResponse> response = duplicateFuture.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        String dupeQueryId = (String) response.getBody().getResult();
        
        // make sure an audit message was sent
        assertAuditSent(dupeQueryId);
        
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.DEFINE,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                queryStatus);
        // @formatter:on
        
        QueryStatus dupeQueryStatus = queryStorageCache.getQueryStatus(dupeQueryId);
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CREATE,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                dupeQueryStatus);
        // @formatter:on
        
        // make sure the original query is unchanged
        Assertions.assertEquals(TEST_QUERY_STRING, queryStatus.getQuery().getQuery());
        Assertions.assertEquals(TEST_QUERY_AUTHORIZATIONS, queryStatus.getQuery().getQueryAuthorizations());
        Assertions.assertEquals(TEST_QUERY_BEGIN, DefaultQueryParameters.formatDate(queryStatus.getQuery().getBeginDate()));
        Assertions.assertEquals(TEST_QUERY_END, DefaultQueryParameters.formatDate(queryStatus.getQuery().getEndDate()));
        Assertions.assertEquals("EventQuery", queryStatus.getQuery().getQueryLogicName());
        
        // make sure the duplicated query is updated
        Assertions.assertEquals(newQuery, dupeQueryStatus.getQuery().getQuery());
        Assertions.assertEquals(newAuths, dupeQueryStatus.getQuery().getQueryAuthorizations());
        Assertions.assertEquals(newBegin, DefaultQueryParameters.formatDate(dupeQueryStatus.getQuery().getBeginDate()));
        Assertions.assertEquals(newEnd, DefaultQueryParameters.formatDate(dupeQueryStatus.getQuery().getEndDate()));
        Assertions.assertEquals(newLogic, dupeQueryStatus.getQuery().getQueryLogicName());
        
        // verify that no events were published
        Assertions.assertEquals(1, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                dupeQueryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testDuplicateFailure_invalidUpdate() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // define a valid query
        String queryId = defineQuery(authUser, createParams());
        
        String newLogic = "SomeBogusLogic";
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        updateParams.set(QUERY_LOGIC_NAME, newLogic);
        
        mockServer.reset();
        auditNotSentSetup();
        
        // duplicate the query
        UriComponents uri = createUri(queryId + "/duplicate");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, updateParams, null, HttpMethod.POST, uri);
        
        // make the duplicate call asynchronously
        Future<ResponseEntity<VoidResponse>> duplicateFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = duplicateFuture.get();
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        // make sure an audit message wasn't sent
        assertAuditNotSent();
        
        // verify that no events were published
        Assertions.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testDuplicateFailure_queryNotFound() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        String queryId = UUID.randomUUID().toString();
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        
        mockServer.reset();
        auditNotSentSetup();
        
        // duplicate the query
        UriComponents uri = createUri(queryId + "/duplicate");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, updateParams, null, HttpMethod.POST, uri);
        
        // make the duplicate call asynchronously
        Future<ResponseEntity<VoidResponse>> duplicateFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = duplicateFuture.get();
        
        Assertions.assertEquals(404, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "No query object matches this id. " + queryId,
                "Exception with no cause caught",
                "404-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // make sure an audit message wasn't sent
        assertAuditNotSent();
        
        // verify that no events were published
        Assertions.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testDuplicateFailure_ownershipFailure() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        DatawaveUserDetails altAuthUser = createAltUserDetails();
        
        // define a valid query
        String queryId = defineQuery(authUser, createParams());
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        
        mockServer.reset();
        auditNotSentSetup();
        
        // duplicate the query
        UriComponents uri = createUri(queryId + "/duplicate");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(altAuthUser, updateParams, null, HttpMethod.POST, uri);
        
        // make the duplicate call asynchronously
        Future<ResponseEntity<VoidResponse>> duplicateFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = duplicateFuture.get();
        
        Assertions.assertEquals(401, response.getStatusCodeValue());
        
        // make sure an audit message wasn't sent
        assertAuditNotSent();
        
        // @formatter:off
        assertQueryException(
                "Current user does not match user that defined query. altuserdn != userdn",
                "Exception with no cause caught",
                "401-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        // make sure the query was not updated
        Assertions.assertEquals(TEST_QUERY_STRING, queryStatus.getQuery().getQuery());
        
        // verify that no events were published
        Assertions.assertEquals(0, queryRequestEvents.size());
    }
}
