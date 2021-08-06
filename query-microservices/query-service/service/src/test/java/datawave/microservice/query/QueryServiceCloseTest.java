package datawave.microservice.query;

import com.google.common.collect.Iterables;
import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.VoidResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryServiceTest", RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE})
public class QueryServiceCloseTest extends AbstractQueryServiceTest {
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void teardown() throws Exception {
        super.teardown();
    }
    
    @Test
    public void testCloseSuccess() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = createQuery(authUser, createParams());
        
        // close the query
        Future<ResponseEntity<VoidResponse>> closeFuture = closeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> closeResponse = closeFuture.get();
        
        Assert.assertEquals(200, closeResponse.getStatusCodeValue());
        
        // verify that query status was created correctly
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CLOSED,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                queryStatus);
        // @formatter:on
        
        // verify that the result queue is gone
        Assert.assertFalse(queryQueueManager.queueExists(queryId));
        
        // verify that the query tasks are still present
        assertTasksCreated(queryId);
        
        // verify that the close event was published
        Assert.assertEquals(2, queryRequestEvents.size());
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
        // @formatter:on
    }
    
    @Test
    public void testCloseSuccess_activeNextCall() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = createQuery(authUser, createParams());
        
        // call next on the query
        Future<ResponseEntity<BaseResponse>> nextFuture = nextQuery(authUser, queryId);
        
        boolean nextCallActive = queryStorageCache.getQueryStatus(queryId).getActiveNextCalls() > 0;
        while (!nextCallActive) {
            try {
                nextFuture.get(500, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                nextCallActive = queryStorageCache.getQueryStatus(queryId).getActiveNextCalls() > 0;
                if (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - currentTimeMillis) > 5) {
                    throw e;
                }
            }
        }
        
        // close the query
        Future<ResponseEntity<VoidResponse>> closeFuture = closeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> closeResponse = closeFuture.get();
        
        Assert.assertEquals(200, closeResponse.getStatusCodeValue());
        
        // verify that query status was created correctly
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CLOSED,
                0,
                0,
                1,
                0,
                currentTimeMillis,
                queryStatus);
        // @formatter:on
        
        // verify that the result queue is still present
        Assert.assertTrue(queryQueueManager.queueExists(queryId));
        
        // send enough results to return a page
        // pump enough results into the queue to trigger a complete page
        int pageSize = queryStorageCache.getQueryStatus(queryId).getQuery().getPagesize();
        
        // test field value pairings
        MultiValueMap<String,String> fieldValues = new LinkedMultiValueMap<>();
        fieldValues.add("LOKI", "ALLIGATOR");
        fieldValues.add("LOKI", "CLASSIC");
        
        // @formatter:off
        publishEventsToQueue(
                queryId,
                pageSize,
                fieldValues,
                "ALL");
        // @formatter:on
        
        // wait for the next call to return
        nextFuture.get();
        
        // verify that the result queue is now gone
        Assert.assertFalse(queryQueueManager.queueExists(queryId));
        
        // verify that the query tasks are still present
        assertTasksCreated(queryId);
        
        // verify that the close event was published
        Assert.assertEquals(3, queryRequestEvents.size());
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
                "executor-unassigned:**",
                QueryRequest.Method.CLOSE,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testCloseFailure_queryNotFound() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        String queryId = UUID.randomUUID().toString();
        
        // close the query
        Future<ResponseEntity<VoidResponse>> closeFuture = closeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> closeResponse = closeFuture.get();
        
        Assert.assertEquals(404, closeResponse.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "No query object matches this id. " + queryId,
                "Exception with no cause caught",
                "404-1",
                Iterables.getOnlyElement(closeResponse.getBody().getExceptions()));
        // @formatter:on
        
    }
    
    @Test
    public void testCloseFailure_ownershipFailure() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        ProxiedUserDetails altAuthUser = createAltUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // make the close call as an alternate user asynchronously
        Future<ResponseEntity<VoidResponse>> future = closeQuery(altAuthUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = future.get();
        
        Assert.assertEquals(401, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Current user does not match user that defined query. altuserdn != userdn",
                "Exception with no cause caught",
                "401-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that the next events were published
        Assert.assertEquals(1, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testCloseFailure_queryNotRunning() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // close the query
        Future<ResponseEntity<VoidResponse>> closeFuture = closeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> closeResponse = closeFuture.get();
        
        Assert.assertEquals(200, closeResponse.getStatusCodeValue());
        
        // try to close the query again
        closeFuture = closeQuery(authUser, queryId);
        
        // the response should come back right away
        closeResponse = closeFuture.get();
        
        Assert.assertEquals(400, closeResponse.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Cannot call close on a query that is not running",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(closeResponse.getBody().getExceptions()));
        // @formatter:on
        
        // verify that the next events were published
        Assert.assertEquals(2, queryRequestEvents.size());
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
        // @formatter:on
    }
    
    @Test
    public void testAdminCloseSuccess() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        ProxiedUserDetails adminUser = createAltUserDetails(Arrays.asList("AuthorizedUser", "Administrator"), null);
        
        // create a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = createQuery(authUser, createParams());
        
        // close the query as the admin user
        Future<ResponseEntity<VoidResponse>> closeFuture = adminCloseQuery(adminUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> closeResponse = closeFuture.get();
        
        Assert.assertEquals(200, closeResponse.getStatusCodeValue());
        
        // verify that query status was created correctly
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        // @formatter:off
        assertQueryStatus(
                QueryStatus.QUERY_STATE.CLOSED,
                0,
                0,
                0,
                0,
                currentTimeMillis,
                queryStatus);
        // @formatter:on
        
        // verify that the result queue is gone
        Assert.assertFalse(queryQueueManager.queueExists(queryId));
        
        // verify that the query tasks are still present
        assertTasksCreated(queryId);
        
        // verify that the close event was published
        Assert.assertEquals(2, queryRequestEvents.size());
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
        // @formatter:on
    }
    
    @Test
    public void testAdminCloseFailure_notAdminUser() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        long currentTimeMillis = System.currentTimeMillis();
        String queryId = createQuery(authUser, createParams());
        
        UriComponents uri = createUri(queryId + "/adminClose");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.PUT, uri);
        
        // close the query
        Future<ResponseEntity<String>> closeFuture = Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, String.class));
        
        // the response should come back right away
        ResponseEntity<String> closeResponse = closeFuture.get();
        
        Assert.assertEquals(403, closeResponse.getStatusCodeValue());
        
        // verify that the create event was published
        Assert.assertEquals(1, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testAdminCloseAllSuccess() throws Exception {
        ProxiedUserDetails adminUser = createUserDetails(Arrays.asList("AuthorizedUser", "Administrator"), null);
        
        // create a bunch of queries
        long currentTimeMillis = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            String queryId = createQuery(adminUser, createParams());
            mockServer.reset();
            
            // @formatter:off
            assertQueryRequestEvent(
                    "executor-unassigned:**",
                    QueryRequest.Method.CREATE,
                    queryId,
                    queryRequestEvents.removeLast());
            // @formatter:on
        }
        
        // close all queries as the admin user
        Future<ResponseEntity<VoidResponse>> closeFuture = adminCloseAllQueries(adminUser);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> closeResponse = closeFuture.get();
        
        Assert.assertEquals(200, closeResponse.getStatusCodeValue());
        
        // verify that query status was created correctly
        List<QueryStatus> queryStatusList = queryStorageCache.getQueryStatus();
        
        for (QueryStatus queryStatus : queryStatusList) {
            // @formatter:off
            assertQueryStatus(
                    QueryStatus.QUERY_STATE.CLOSED,
                    0,
                    0,
                    0,
                    0,
                    currentTimeMillis,
                    queryStatus);
            // @formatter:on
            
            String queryId = queryStatus.getQueryKey().getQueryId();
            
            // verify that the result queue is gone
            Assert.assertFalse(queryQueueManager.queueExists(queryStatus.getQueryKey().getQueryId()));
            
            // verify that the query tasks are still present
            assertTasksCreated(queryStatus.getQueryKey().getQueryId());
            
            // @formatter:off
            assertQueryRequestEvent(
                    "executor-unassigned:**",
                    QueryRequest.Method.CLOSE,
                    queryId,
                    queryRequestEvents.removeLast());
            // @formatter:on
        }
        
        // verify that there are no more events
        Assert.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testAdminCloseAllFailure_notAdminUser() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
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
        
        // close all queries as the admin user
        UriComponents uri = createUri("/adminCloseAll");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.PUT, uri);
        
        // make the next call asynchronously
        Future<ResponseEntity<String>> closeFuture = Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, String.class));
        
        // the response should come back right away
        ResponseEntity<String> closeResponse = closeFuture.get();
        
        Assert.assertEquals(403, closeResponse.getStatusCodeValue());
        
        // verify that query status was created correctly
        List<QueryStatus> queryStatusList = queryStorageCache.getQueryStatus();
        
        // verify that none of the queries were canceled
        for (QueryStatus queryStatus : queryStatusList) {
            // @formatter:off
            assertQueryStatus(
                    QueryStatus.QUERY_STATE.CREATED,
                    0,
                    0,
                    0,
                    0,
                    currentTimeMillis,
                    queryStatus);
            // @formatter:on
            
            // verify that the result queue is still present
            Assert.assertTrue(queryQueueManager.queueExists(queryStatus.getQueryKey().getQueryId()));
            
            // verify that the query tasks are still present
            assertTasksCreated(queryStatus.getQueryKey().getQueryId());
        }
        
        // verify that there are no more events
        Assert.assertEquals(0, queryRequestEvents.size());
    }
}
