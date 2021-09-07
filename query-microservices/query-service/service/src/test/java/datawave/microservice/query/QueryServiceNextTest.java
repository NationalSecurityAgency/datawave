package datawave.microservice.query;

import com.google.common.collect.Iterables;
import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.TaskStates;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.VoidResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryServiceTest", RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE})
public class QueryServiceNextTest extends AbstractQueryServiceTest {
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void teardown() throws Exception {
        super.teardown();
    }
    
    @Test
    public void testNextSuccess() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // pump enough results into the queue to trigger a complete page
        int pageSize = queryStorageCache.getQueryStatus(queryId).getQuery().getPagesize();
        
        // test field value pairings
        MultiValueMap<String,String> fieldValues = new LinkedMultiValueMap<>();
        fieldValues.add("LOKI", "ALLIGATOR");
        fieldValues.add("LOKI", "CLASSIC");
        
        // @formatter:off
        publishEventsToQueue(
                queryId,
                (int) (1.5 * pageSize),
                fieldValues,
                "ALL");
        // @formatter:on
        
        // make the next call asynchronously
        Future<ResponseEntity<BaseResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<BaseResponse> response = future.get();
        
        Assert.assertEquals(200, response.getStatusCodeValue());
        
        // verify some headers
        Assert.assertEquals("1", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-page-number"))));
        Assert.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-Partial-Results"))));
        Assert.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-last-page"))));
        
        DefaultEventQueryResponse queryResponse = (DefaultEventQueryResponse) response.getBody();
        
        // verify the query response
        // @formatter:off
        assertQueryResponse(
                queryId,
                "EventQuery",
                1,
                false,
                Long.parseLong(Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-OperationTimeInMS")))),
                1,
                Collections.singletonList("LOKI"),
                pageSize,
                Objects.requireNonNull(queryResponse));
        // @formatter:on
        
        // validate one of the events
        DefaultEvent event = (DefaultEvent) queryResponse.getEvents().get(0);
        // @formatter:off
        assertDefaultEvent(
                Arrays.asList("LOKI", "LOKI"),
                Arrays.asList("ALLIGATOR", "CLASSIC"),
                event);
        // @formatter:on
        
        // verify that the next event was published
        Assert.assertEquals(2, queryRequestEvents.size());
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
        // @formatter:on
    }
    
    @Test
    public void testNextSuccess_multiplePages() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // pump enough results into the queue to trigger two complete pages
        int pageSize = queryStorageCache.getQueryStatus(queryId).getQuery().getPagesize();
        
        // test field value pairings
        MultiValueMap<String,String> fieldValues = new LinkedMultiValueMap<>();
        fieldValues.add("LOKI", "ALLIGATOR");
        fieldValues.add("LOKI", "CLASSIC");
        
        // verify that the create event was published
        Assert.assertEquals(1, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:off

        for (int page = 1; page <= 2; page++) {
            // TODO: We have to generate the results in between next calls because the test queue manager does not handle requeueing of unused messages :(
            // @formatter:off
            publishEventsToQueue(
                    queryId,
                    pageSize,
                    fieldValues,
                    "ALL");
            // @formatter:on
            
            // make the next call asynchronously
            Future<ResponseEntity<BaseResponse>> future = nextQuery(authUser, queryId);
            
            // the response should come back right away
            ResponseEntity<BaseResponse> response = future.get();
            
            Assert.assertEquals(200, response.getStatusCodeValue());
            
            // verify some headers
            Assert.assertEquals(Integer.toString(page), Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-page-number"))));
            Assert.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-Partial-Results"))));
            Assert.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-last-page"))));
            
            DefaultEventQueryResponse queryResponse = (DefaultEventQueryResponse) response.getBody();
            
            // verify the query response
            // @formatter:off
            assertQueryResponse(
                    queryId,
                    "EventQuery",
                    page,
                    false,
                    Long.parseLong(Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-OperationTimeInMS")))),
                    1,
                    Collections.singletonList("LOKI"),
                    pageSize,
                    Objects.requireNonNull(queryResponse));
            // @formatter:on
            
            // validate one of the events
            DefaultEvent event = (DefaultEvent) queryResponse.getEvents().get(0);
            // @formatter:off
            assertDefaultEvent(
                    Arrays.asList("LOKI", "LOKI"),
                    Arrays.asList("ALLIGATOR", "CLASSIC"),
                    event);
            // @formatter:on
            
            // verify that the next event was published
            Assert.assertEquals(1, queryRequestEvents.size());
            // @formatter:off
            assertQueryRequestEvent(
                    "executor-unassigned:**",
                    QueryRequest.Method.NEXT,
                    queryId,
                    queryRequestEvents.removeLast());
            // @formatter:on
        }
    }
    
    @Test
    public void testNextSuccess_cancelPartialResults() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // pump enough results into the queue to trigger a complete page
        int pageSize = queryStorageCache.getQueryStatus(queryId).getQuery().getPagesize();
        
        // test field value pairings
        MultiValueMap<String,String> fieldValues = new LinkedMultiValueMap<>();
        fieldValues.add("LOKI", "ALLIGATOR");
        fieldValues.add("LOKI", "CLASSIC");
        
        int numEvents = (int) (0.5 * pageSize);
        
        // @formatter:off
        publishEventsToQueue(
                queryId,
                numEvents,
                fieldValues,
                "ALL");
        // @formatter:on
        
        // make the next call asynchronously
        Future<ResponseEntity<BaseResponse>> nextFuture = nextQuery(authUser, queryId);
        
        // make sure all events were consumed before canceling
        while (queryQueueManager.getQueueSize(queryId) != 0) {
            Thread.sleep(100);
        }
        
        // cancel the query so that it returns partial results
        Future<ResponseEntity<VoidResponse>> cancelFuture = cancelQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assert.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        // the response should come back right away
        ResponseEntity<BaseResponse> nextResponse = nextFuture.get();
        
        Assert.assertEquals(200, nextResponse.getStatusCodeValue());
        
        // verify some headers
        Assert.assertEquals("1", Iterables.getOnlyElement(Objects.requireNonNull(nextResponse.getHeaders().get("X-query-page-number"))));
        Assert.assertEquals("true", Iterables.getOnlyElement(Objects.requireNonNull(nextResponse.getHeaders().get("X-Partial-Results"))));
        Assert.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(nextResponse.getHeaders().get("X-query-last-page"))));
        
        DefaultEventQueryResponse queryResponse = (DefaultEventQueryResponse) nextResponse.getBody();
        
        // verify the query response
        // @formatter:off
        assertQueryResponse(
                queryId,
                "EventQuery",
                1,
                true,
                Long.parseLong(Iterables.getOnlyElement(Objects.requireNonNull(nextResponse.getHeaders().get("X-OperationTimeInMS")))),
                1,
                Collections.singletonList("LOKI"),
                numEvents,
                Objects.requireNonNull(queryResponse));
        // @formatter:on
        
        // validate one of the events
        DefaultEvent event = (DefaultEvent) queryResponse.getEvents().get(0);
        // @formatter:off
        assertDefaultEvent(
                Arrays.asList("LOKI", "LOKI"),
                Arrays.asList("ALLIGATOR", "CLASSIC"),
                event);
        // @formatter:on
        
        // verify that the next events were published
        Assert.assertEquals(4, queryRequestEvents.size());
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
    public void testNextSuccess_maxResults() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // pump enough results into the queue to trigger two complete pages
        int pageSize = queryStorageCache.getQueryStatus(queryId).getQuery().getPagesize();
        
        // test field value pairings
        MultiValueMap<String,String> fieldValues = new LinkedMultiValueMap<>();
        fieldValues.add("LOKI", "ALLIGATOR");
        fieldValues.add("LOKI", "CLASSIC");
        
        // verify that the create event was published
        Assert.assertEquals(1, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:on
        
        for (int page = 1; page <= 4; page++) {
            // TODO: We have to generate the results in between next calls because the test queue manager does not handle requeueing of unused messages :(
            // @formatter:off
            publishEventsToQueue(
                    queryId,
                    pageSize,
                    fieldValues,
                    "ALL");
            // @formatter:on
            
            // make the next call asynchronously
            Future<ResponseEntity<BaseResponse>> future = nextQuery(authUser, queryId);
            
            // the response should come back right away
            ResponseEntity<BaseResponse> response = future.get();
            
            if (page != 4) {
                Assert.assertEquals(200, response.getStatusCodeValue());
            } else {
                Assert.assertEquals(204, response.getStatusCodeValue());
            }
            
            if (page != 4) {
                // verify some headers
                Assert.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-Partial-Results"))));
                Assert.assertEquals(Integer.toString(page), Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-page-number"))));
                Assert.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-last-page"))));
                
                DefaultEventQueryResponse queryResponse = (DefaultEventQueryResponse) response.getBody();
                
                // verify the query response
                // @formatter:off
                assertQueryResponse(
                        queryId,
                        "EventQuery",
                        page,
                        false,
                        Long.parseLong(Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-OperationTimeInMS")))),
                        1,
                        Collections.singletonList("LOKI"),
                        pageSize,
                        Objects.requireNonNull(queryResponse));
                // @formatter:on
                
                // validate one of the events
                DefaultEvent event = (DefaultEvent) queryResponse.getEvents().get(0);
                // @formatter:off
                assertDefaultEvent(
                        Arrays.asList("LOKI", "LOKI"),
                        Arrays.asList("ALLIGATOR", "CLASSIC"),
                        event);
                // @formatter:on
                
                // verify that the next event was published
                Assert.assertEquals(1, queryRequestEvents.size());
                // @formatter:off
                assertQueryRequestEvent(
                        "executor-unassigned:**",
                        QueryRequest.Method.NEXT,
                        queryId,
                        queryRequestEvents.removeLast());
                // @formatter:on
            } else {
                Assert.assertNull(response.getBody());
                
                // verify that the next and close events were published
                Assert.assertEquals(2, queryRequestEvents.size());
                // @formatter:off
                assertQueryRequestEvent(
                        "executor-unassigned:**",
                        QueryRequest.Method.NEXT,
                        queryId,
                        queryRequestEvents.removeLast());
                // @formatter:on
                // @formatter:off
                assertQueryRequestEvent(
                        "executor-unassigned:**",
                        QueryRequest.Method.CLOSE,
                        queryId,
                        queryRequestEvents.removeLast());
                // @formatter:on
            }
        }
        
        // make the next call asynchronously
        Future<ResponseEntity<BaseResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<BaseResponse> response = future.get();
        
        Assert.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Cannot call next on a query that is not running",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
    }
    
    @Test
    public void testNextSuccess_noResults() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // remove the task states to make it appear that the executor has finished
        TaskStates taskStates = queryStorageCache.getTaskStates(queryId);
        taskStates.getTaskStates().remove(TaskStates.TASK_STATE.READY);
        queryStorageCache.updateTaskStates(taskStates);
        
        // make the next call asynchronously
        Future<ResponseEntity<BaseResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<BaseResponse> response = future.get();
        
        Assert.assertEquals(204, response.getStatusCodeValue());
        Assert.assertNull(response.getBody());
        
        // verify that the next event was published
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
    public void testNextFailure_queryNotFound() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        String queryId = UUID.randomUUID().toString();
        
        // make the next call asynchronously
        Future<ResponseEntity<BaseResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<BaseResponse> response = future.get();
        
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
    public void testNextFailure_queryNotRunning() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // cancel the query so that it returns partial results
        Future<ResponseEntity<VoidResponse>> cancelFuture = cancelQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assert.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        // make the next call asynchronously
        Future<ResponseEntity<BaseResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<BaseResponse> response = future.get();
        
        Assert.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Cannot call next on a query that is not running",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that the next events were published
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
    public void testNextFailure_ownershipFailure() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        ProxiedUserDetails altAuthUser = createAltUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // make the next call as an alternate user asynchronously
        Future<ResponseEntity<BaseResponse>> future = nextQuery(altAuthUser, queryId);
        
        // the response should come back right away
        ResponseEntity<BaseResponse> response = future.get();
        
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
    public void testNextFailure_timeout() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // make the next call asynchronously
        Future<ResponseEntity<BaseResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back after the configured timeout (5 seconds)
        ResponseEntity<BaseResponse> response = future.get();
        
        Assert.assertEquals(500, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Query timed out. " + queryId + " timed out.",
                "Exception with no cause caught",
                "500-27",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
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
                QueryRequest.Method.NEXT,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testNextFailure_nextOnDefined() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // define a valid query
        String queryId = defineQuery(authUser, createParams());
        
        // make the next call
        Future<ResponseEntity<BaseResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<BaseResponse> response = future.get();
        
        Assert.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Cannot call next on a query that is not running",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that no events were published
        Assert.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testNextFailure_nextOnClosed() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // close the query
        Future<ResponseEntity<VoidResponse>> closeFuture = closeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> closeResponse = closeFuture.get();
        
        Assert.assertEquals(200, closeResponse.getStatusCodeValue());
        
        // make the next call asynchronously
        Future<ResponseEntity<BaseResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<BaseResponse> response = future.get();
        
        Assert.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Cannot call next on a query that is not running",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that no events were published
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
    public void testNextFailure_nextOnCanceled() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // cancel the query
        Future<ResponseEntity<VoidResponse>> cancelFuture = cancelQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assert.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        // make the next call asynchronously
        Future<ResponseEntity<BaseResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<BaseResponse> response = future.get();
        
        Assert.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Cannot call next on a query that is not running",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that the cancel event was published
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
}
