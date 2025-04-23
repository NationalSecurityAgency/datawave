package datawave.microservice.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
import org.springframework.web.util.UriComponents;

import com.google.common.collect.Iterables;

import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.VoidResponse;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryStarterDefaults", "QueryStarterOverrides", "QueryServiceTest", RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE})
@ContextConfiguration(classes = {QueryService.class})
public class QueryServiceCancelTest extends AbstractQueryServiceTest {
    
    @Test
    public void testCancelSuccess() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = createQuery(authUser, createParams());
        
        // cancel the query
        Future<ResponseEntity<VoidResponse>> cancelFuture = cancelQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assertions.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        // verify that query status was created correctly
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
        
        // verify that the query tasks are still present
        assertTasksCreated(queryId);
        
        // verify that the cancel event was published
        Assertions.assertEquals(3, queryRequestEvents.size());
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
        // @formatter:on
    }
    
    @Test
    public void testCancelSuccess_activeNextCall() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = createQuery(authUser, createParams());
        
        // call next on the query
        Future<ResponseEntity<DefaultEventQueryResponse>> nextFuture = nextQuery(authUser, queryId);
        
        boolean nextCallActive = queryStorageCache.getQueryStatus(queryId).getActiveNextCalls() > 0;
        while (!nextCallActive) {
            try {
                nextFuture.get(500, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                nextCallActive = queryStorageCache.getQueryStatus(queryId).getActiveNextCalls() > 0;
                if ((System.currentTimeMillis() - currentTimeMillis) > TEST_WAIT_TIME_MILLIS) {
                    throw e;
                }
            }
        }
        
        // cancel the query
        Future<ResponseEntity<VoidResponse>> cancelFuture = cancelQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assertions.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        // wait for the next call to drop out before checking status
        // since we canceled, this should quit immediately, but just in case, we add a timeout
        nextFuture.get(10, TimeUnit.SECONDS);
        
        // verify that query status was created correctly
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
        
        // wait for the next call to return
        nextFuture.get();
        
        // verify that the query tasks are still present
        assertTasksCreated(queryId);
        
        // verify that the close event was published
        Assertions.assertEquals(4, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.NEXT,
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
        // @formatter:on
    }
    
    @Test
    public void testCancelFailure_queryNotFound() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        String queryId = UUID.randomUUID().toString();
        
        // cancel the query
        Future<ResponseEntity<VoidResponse>> cancelFuture = cancelQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assertions.assertEquals(404, cancelResponse.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "No query object matches this id. " + queryId,
                "Exception with no cause caught",
                "404-1",
                Iterables.getOnlyElement(cancelResponse.getBody().getExceptions()));
        // @formatter:on
        
    }
    
    @Test
    public void testCancelFailure_ownershipFailure() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        DatawaveUserDetails altAuthUser = createAltUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // make the cancel call as an alternate user asynchronously
        Future<ResponseEntity<VoidResponse>> future = cancelQuery(altAuthUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = future.get();
        
        Assertions.assertEquals(401, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Current user does not match user that defined query. altuserdn != userdn",
                "Exception with no cause caught",
                "401-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that the next events were published
        Assertions.assertEquals(1, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testCancelFailure_queryNotRunning() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // cancel the query
        Future<ResponseEntity<VoidResponse>> cancelFuture = cancelQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assertions.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        // try to cancel the query again
        cancelFuture = cancelQuery(authUser, queryId);
        
        // the response should come back right away
        cancelResponse = cancelFuture.get();
        
        Assertions.assertEquals(400, cancelResponse.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Cannot call cancel on a query that is not running",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(cancelResponse.getBody().getExceptions()));
        // @formatter:on
        
        // verify that the next events were published
        Assertions.assertEquals(3, queryRequestEvents.size());
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
        // @formatter:on
    }
    
    @Test
    public void testAdminCancelSuccess() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        DatawaveUserDetails adminUser = createAltUserDetails(Arrays.asList("AuthorizedUser", "Administrator"), null);
        
        // create a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = createQuery(authUser, createParams());
        
        // cancel the query as the admin user
        Future<ResponseEntity<VoidResponse>> cancelFuture = adminCancelQuery(adminUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assertions.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        // verify that query status was created correctly
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
        
        // verify that the query tasks are still present
        assertTasksCreated(queryId);
        
        // verify that the cancel event was published
        Assertions.assertEquals(3, queryRequestEvents.size());
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
        // @formatter:on
    }
    
    @Test
    public void testAdminCancelFailure_notAdminUser() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        UriComponents uri = createUri(queryId + "/adminCancel");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.PUT, uri);
        
        // cancel the query
        Future<ResponseEntity<String>> closeFuture = Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, String.class));
        
        // the response should come back right away
        ResponseEntity<String> closeResponse = closeFuture.get();
        
        Assertions.assertEquals(403, closeResponse.getStatusCodeValue());
        
        // verify that the create event was published
        Assertions.assertEquals(1, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testAdminCancelAllSuccess() throws Exception {
        DatawaveUserDetails adminUser = createUserDetails(Arrays.asList("AuthorizedUser", "Administrator"), null);
        
        // create a bunch of queries
        List<String> queryIds = new ArrayList<>();
        long currentTimeMillis = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            String queryId = createQuery(adminUser, createParams());
            mockServer.reset();
            
            queryIds.add(queryId);
            
            // @formatter:off
            assertQueryRequestEvent(
                    "executor-unassigned:**",
                    QueryRequest.Method.CREATE,
                    queryId,
                    queryRequestEvents.removeLast());
            // @formatter:on
        }
        
        // close all queries as the admin user
        Future<ResponseEntity<VoidResponse>> cancelFuture = adminCancelAllQueries(adminUser);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assertions.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        // verify that query status was created correctly
        List<QueryStatus> queryStatusList = queryStorageCache.getQueryStatus();
        
        for (QueryStatus queryStatus : queryStatusList) {
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
            
            String queryId = queryStatus.getQueryKey().getQueryId();
            
            // verify that the query tasks are still present
            assertTasksCreated(queryStatus.getQueryKey().getQueryId());
            
            // @formatter:off
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
            // @formatter:on
        }
        
        // verify that there are no more events
        Assertions.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testAdminCancelAllFailure_notAdminUser() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a bunch of queries
        List<String> queryIds = new ArrayList<>();
        long currentTimeMillis = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            String queryId = createQuery(authUser, createParams());
            mockServer.reset();
            
            queryIds.add(queryId);
            
            // @formatter:off
            assertQueryRequestEvent(
                    "executor-unassigned:**",
                    QueryRequest.Method.CREATE,
                    queryId,
                    queryRequestEvents.removeLast());
            // @formatter:on
        }
        
        // cancel all queries as the admin user
        UriComponents uri = createUri("/adminCancelAll");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.PUT, uri);
        
        // make the next call asynchronously
        Future<ResponseEntity<String>> cancelFuture = Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, String.class));
        
        // the response should come back right away
        ResponseEntity<String> cancelResponse = cancelFuture.get();
        
        Assertions.assertEquals(403, cancelResponse.getStatusCodeValue());
        
        // verify that query status was created correctly
        List<QueryStatus> queryStatusList = queryStorageCache.getQueryStatus();
        
        // verify that none of the queries were canceled
        for (QueryStatus queryStatus : queryStatusList) {
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
            
            // verify that the query tasks are still present
            assertTasksCreated(queryStatus.getQueryKey().getQueryId());
        }
        
        // verify that there are no more events
        Assertions.assertEquals(0, queryRequestEvents.size());
    }
}
