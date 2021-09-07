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
import org.springframework.web.util.UriComponents;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryServiceTest", RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE})
public class QueryServiceRemoveTest extends AbstractQueryServiceTest {
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void teardown() throws Exception {
        super.teardown();
    }
    
    @Test
    public void testRemoveSuccess_removeOnDefined() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // define a valid query
        String queryId = defineQuery(authUser, createParams());
        
        // remove the query
        Future<ResponseEntity<VoidResponse>> removeFuture = removeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = removeFuture.get();
        
        Assert.assertEquals(200, response.getStatusCodeValue());
        
        // verify that original query was removed
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        Assert.assertNull(queryStatus);
        
        // verify that events were published
        Assert.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testRemoveSuccess_removeOnClosed() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // close the query
        Future<ResponseEntity<VoidResponse>> closeFuture = closeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> closeResponse = closeFuture.get();
        
        Assert.assertEquals(200, closeResponse.getStatusCodeValue());
        
        // remove the query
        Future<ResponseEntity<VoidResponse>> removeFuture = removeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = removeFuture.get();
        
        Assert.assertEquals(200, response.getStatusCodeValue());
        
        // verify that original query was removed
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        Assert.assertNull(queryStatus);
        
        // verify that events were published
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
    public void testRemoveSuccess_removeOnCanceled() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // cancel the query
        Future<ResponseEntity<VoidResponse>> cancelFuture = cancelQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assert.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        // remove the query
        Future<ResponseEntity<VoidResponse>> removeFuture = removeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = removeFuture.get();
        
        Assert.assertEquals(200, response.getStatusCodeValue());
        
        // verify that original query was removed
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        Assert.assertNull(queryStatus);
        
        // verify that events were published
        Assert.assertEquals(3, queryRequestEvents.size());
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
    public void testRemoveFailure_removeOnCreated() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // remove the query
        Future<ResponseEntity<VoidResponse>> removeFuture = removeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = removeFuture.get();
        
        Assert.assertEquals(400, response.getStatusCodeValue());
        
        Assert.assertEquals("Cannot remove a running query.", Iterables.getOnlyElement(response.getBody().getExceptions()).getMessage());
        
        // verify that original query was not removed
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        Assert.assertNotNull(queryStatus);
        
        // verify that events were published
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
    public void testRemoveFailure_removeOnClosedActiveNext() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // call next on the query
        nextQuery(authUser, queryId);
        
        // close the query
        Future<ResponseEntity<VoidResponse>> closeFuture = closeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> closeResponse = closeFuture.get();
        
        Assert.assertEquals(200, closeResponse.getStatusCodeValue());
        
        // remove the query
        Future<ResponseEntity<VoidResponse>> removeFuture = removeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = removeFuture.get();
        
        Assert.assertEquals(400, response.getStatusCodeValue());
        
        Assert.assertEquals("Cannot remove a running query.", Iterables.getOnlyElement(response.getBody().getExceptions()).getMessage());
        
        // verify that original query was not removed
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        Assert.assertNotNull(queryStatus);
        
        // verify that events were published
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
    public void testRemoveFailure_queryNotFound() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        String queryId = UUID.randomUUID().toString();
        
        // remove the query
        UriComponents uri = createUri(queryId + "/remove");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.DELETE, uri);
        
        // close the query
        Future<ResponseEntity<BaseResponse>> resetFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, BaseResponse.class));
        
        // the response should come back right away
        ResponseEntity<BaseResponse> response = resetFuture.get();
        
        Assert.assertEquals(404, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "No query object matches this id. " + queryId,
                "Exception with no cause caught",
                "404-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that no events were published
        Assert.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testRemoveFailure_ownershipFailure() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        ProxiedUserDetails altAuthUser = createAltUserDetails();
        
        // define a valid query
        String queryId = defineQuery(authUser, createParams());
        
        // remove the query
        UriComponents uri = createUri(queryId + "/remove");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(altAuthUser, null, null, HttpMethod.DELETE, uri);
        
        // close the query
        Future<ResponseEntity<BaseResponse>> resetFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, BaseResponse.class));
        
        // the response should come back right away
        ResponseEntity<BaseResponse> response = resetFuture.get();
        
        Assert.assertEquals(401, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Current user does not match user that defined query. altuserdn != userdn",
                "Exception with no cause caught",
                "401-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that no events were published
        Assert.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testAdminRemoveSuccess() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        ProxiedUserDetails adminUser = createAltUserDetails(Arrays.asList("AuthorizedUser", "Administrator"), null);
        
        // define a valid query
        String queryId = defineQuery(authUser, createParams());
        
        // remove the query
        Future<ResponseEntity<VoidResponse>> removeFuture = adminRemoveQuery(adminUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = removeFuture.get();
        
        Assert.assertEquals(200, response.getStatusCodeValue());
        
        // verify that original query was removed
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        Assert.assertNull(queryStatus);
        
        // verify that events were published
        Assert.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testAdminRemoveFailure_notAdminUser() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // define a valid query
        String queryId = defineQuery(authUser, createParams());
        
        // remove the query
        UriComponents uri = createUri(queryId + "/adminRemove");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.DELETE, uri);
        
        // remove the queries
        Future<ResponseEntity<String>> removeFuture = Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, String.class));
        
        // the response should come back right away
        ResponseEntity<String> response = removeFuture.get();
        
        Assert.assertEquals(403, response.getStatusCodeValue());
        
        // verify that original query was not removed
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        Assert.assertNotNull(queryStatus);
        
        // verify that events were published
        Assert.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testAdminRemoveAllSuccess() throws Exception {
        ProxiedUserDetails adminUser = createUserDetails(Arrays.asList("AuthorizedUser", "Administrator"), null);
        
        // define a bunch of queries
        for (int i = 0; i < 10; i++) {
            defineQuery(adminUser, createParams());
        }
        
        // remove all queries as the admin user
        Future<ResponseEntity<VoidResponse>> removeFuture = adminRemoveAllQueries(adminUser);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> removeResponse = removeFuture.get();
        
        Assert.assertEquals(200, removeResponse.getStatusCodeValue());
        
        // verify that query status was created correctly
        List<QueryStatus> queryStatusList = queryStorageCache.getQueryStatus();
        
        Assert.assertEquals(0, queryStatusList.size());
        
        // verify that there are no events
        Assert.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testAdminRemoveAllFailure_notAdminUser() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // define a bunch of queries
        for (int i = 0; i < 10; i++) {
            defineQuery(authUser, createParams());
        }
        
        UriComponents uri = createUri("/adminRemoveAll");
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, null, HttpMethod.DELETE, uri);
        
        // remove the queries
        Future<ResponseEntity<String>> removeFuture = Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, String.class));
        
        // the response should come back right away
        ResponseEntity<String> removeResponse = removeFuture.get();
        
        Assert.assertEquals(403, removeResponse.getStatusCodeValue());
        
        // verify that query status was created correctly
        List<QueryStatus> queryStatusList = queryStorageCache.getQueryStatus();
        
        Assert.assertEquals(10, queryStatusList.size());
        
        // verify that there are no events
        Assert.assertEquals(0, queryRequestEvents.size());
    }
}
