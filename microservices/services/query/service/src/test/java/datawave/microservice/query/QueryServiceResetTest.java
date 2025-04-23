package datawave.microservice.query;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
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
public class QueryServiceResetTest extends AbstractQueryServiceTest {
    
    @Test
    public void testResetSuccess_resetOnDefined() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // define a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = defineQuery(authUser, createParams());
        
        mockServer.reset();
        auditSentSetup();
        
        // reset the query
        Future<ResponseEntity<GenericResponse>> resetFuture = resetQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<GenericResponse> response = resetFuture.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        // @formatter:off
        assertGenericResponse(
                true,
                HttpStatus.Series.SUCCESSFUL,
                response);
        // @formatter:on
        
        String resetQueryId = (String) response.getBody().getResult();
        
        // verify that a new query id was created
        Assertions.assertNotEquals(queryId, resetQueryId);
        
        // verify that an audit record was sent
        assertAuditSent(resetQueryId);
        
        // verify that original query was canceled
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
        
        // verify that new query was created
        QueryStatus resetQueryStatus = queryStorageCache.getQueryStatus(resetQueryId);
        
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CREATE,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                resetQueryStatus);
        // @formatter:on
        
        // make sure the queries are equal (ignoring the query id)
        queryStatus.getQuery().setId(resetQueryStatus.getQuery().getId());
        Assertions.assertEquals(queryStatus.getQuery(), resetQueryStatus.getQuery());
        
        // verify that events were published
        Assertions.assertEquals(1, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                resetQueryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testResetSuccess_resetOnCreated() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // define a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = createQuery(authUser, createParams());
        
        mockServer.reset();
        auditSentSetup();
        
        // reset the query
        Future<ResponseEntity<GenericResponse>> resetFuture = resetQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<GenericResponse> response = resetFuture.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        // @formatter:off
        assertGenericResponse(
                true,
                HttpStatus.Series.SUCCESSFUL,
                response);
        // @formatter:on
        
        String resetQueryId = (String) response.getBody().getResult();
        
        // verify that a new query id was created
        Assertions.assertNotEquals(queryId, resetQueryId);
        
        // verify that an audit record was sent
        assertAuditSent(resetQueryId);
        
        // verify that original query was canceled
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
        
        // verify that new query was created
        QueryStatus resetQueryStatus = queryStorageCache.getQueryStatus(resetQueryId);
        
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CREATE,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                resetQueryStatus);
        // @formatter:on
        
        // make sure the queries are equal (ignoring the query id)
        queryStatus.getQuery().setId(resetQueryStatus.getQuery().getId());
        Assertions.assertEquals(queryStatus.getQuery(), resetQueryStatus.getQuery());
        
        // verify that events were published
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
                resetQueryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testResetSuccess_resetOnClosed() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // define a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = createQuery(authUser, createParams());
        
        // close the query
        Future<ResponseEntity<VoidResponse>> closeFuture = closeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> closeResponse = closeFuture.get();
        
        Assertions.assertEquals(200, closeResponse.getStatusCodeValue());
        
        mockServer.reset();
        auditSentSetup();
        
        // reset the query
        Future<ResponseEntity<GenericResponse>> resetFuture = resetQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<GenericResponse> response = resetFuture.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        // @formatter:off
        assertGenericResponse(
                true,
                HttpStatus.Series.SUCCESSFUL,
                response);
        // @formatter:on
        
        String resetQueryId = (String) response.getBody().getResult();
        
        // verify that a new query id was created
        Assertions.assertNotEquals(queryId, resetQueryId);
        
        // verify that an audit record was sent
        assertAuditSent(resetQueryId);
        
        // verify that original query was closed
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
        
        // verify that new query was created
        QueryStatus resetQueryStatus = queryStorageCache.getQueryStatus(resetQueryId);
        
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CREATE,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                resetQueryStatus);
        // @formatter:on
        
        // make sure the queries are equal (ignoring the query id)
        queryStatus.getQuery().setId(resetQueryStatus.getQuery().getId());
        Assertions.assertEquals(queryStatus.getQuery(), resetQueryStatus.getQuery());
        
        // verify that events were published
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
                resetQueryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testResetSuccess_resetOnCanceled() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // define a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = createQuery(authUser, createParams());
        
        // cancel the query
        Future<ResponseEntity<VoidResponse>> cancelFuture = cancelQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assertions.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        mockServer.reset();
        auditSentSetup();
        
        // reset the query
        Future<ResponseEntity<GenericResponse>> resetFuture = resetQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<GenericResponse> response = resetFuture.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        // @formatter:off
        assertGenericResponse(
                true,
                HttpStatus.Series.SUCCESSFUL,
                response);
        // @formatter:on
        
        String resetQueryId = (String) response.getBody().getResult();
        
        // verify that a new query id was created
        Assertions.assertNotEquals(queryId, resetQueryId);
        
        // verify that an audit record was sent
        assertAuditSent(resetQueryId);
        
        // verify that original query was canceled
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
        
        // verify that new query was created
        QueryStatus resetQueryStatus = queryStorageCache.getQueryStatus(resetQueryId);
        
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CREATE,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                resetQueryStatus);
        // @formatter:on
        
        // make sure the queries are equal (ignoring the query id)
        queryStatus.getQuery().setId(resetQueryStatus.getQuery().getId());
        Assertions.assertEquals(queryStatus.getQuery(), resetQueryStatus.getQuery());
        
        // verify that events were published
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
                resetQueryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testResetFailure_queryNotFound() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        String queryId = UUID.randomUUID().toString();
        
        auditNotSentSetup();
        
        // reset the query
        UriComponents uri = createUri(queryId + "/reset");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.PUT, uri);
        
        // close the query
        Future<ResponseEntity<GenericResponse>> resetFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, GenericResponse.class));
        
        // the response should come back right away
        ResponseEntity<GenericResponse> response = resetFuture.get();
        
        Assertions.assertEquals(404, response.getStatusCodeValue());
        
        // make sure no audits were sent
        assertAuditNotSent();
        
        // @formatter:off
        assertQueryException(
                "No query object matches this id. " + queryId,
                "Exception with no cause caught",
                "404-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that no events were published
        Assertions.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testResetFailure_ownershipFailure() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        DatawaveUserDetails altAuthUser = createAltUserDetails();
        
        // define a valid query
        String queryId = createQuery(authUser, createParams());
        
        mockServer.reset();
        auditNotSentSetup();
        
        // reset the query
        UriComponents uri = createUri(queryId + "/reset");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(altAuthUser, null, null, HttpMethod.PUT, uri);
        
        // close the query
        Future<ResponseEntity<GenericResponse>> resetFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, GenericResponse.class));
        
        // the response should come back right away
        ResponseEntity<GenericResponse> response = resetFuture.get();
        
        Assertions.assertEquals(401, response.getStatusCodeValue());
        
        // make sure no audits were sent
        assertAuditNotSent();
        
        // @formatter:off
        assertQueryException(
                "Current user does not match user that defined query. altuserdn != userdn",
                "Exception with no cause caught",
                "401-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that no events were published
        Assertions.assertEquals(1, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
}
