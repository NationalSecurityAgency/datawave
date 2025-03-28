package datawave.microservice.query;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.google.common.collect.Iterables;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.TaskStates;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.VoidResponse;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryStarterDefaults", "QueryStarterOverrides", "QueryServiceTest", RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE})
@ContextConfiguration(classes = {QueryService.class})
public class QueryServiceNextTest extends AbstractQueryServiceTest {
    
    @Test
    public void testNextSuccess() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // pump enough results into the queue to trigger a complete page
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        int pageSize = queryStatus.getQuery().getPagesize();
        
        // test field value pairings
        MultiValueMap<String,String> fieldValues = new LinkedMultiValueMap<>();
        fieldValues.add("LOKI", "ALLIGATOR");
        fieldValues.add("LOKI", "CLASSIC");
        
        // add a config object to the query status, which would normally be added by the executor service
        queryStatus.setConfig(new GenericQueryConfiguration());
        queryStorageCache.updateQueryStatus(queryStatus);
        
        // @formatter:off
        publishEventsToQueue(
                queryId,
                (int) (1.5 * pageSize),
                fieldValues,
                "ALL");
        // @formatter:on
        
        // make the next call asynchronously
        Future<ResponseEntity<DefaultEventQueryResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        // verify some headers
        Assertions.assertEquals("1", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-page-number"))));
        Assertions.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-Partial-Results"))));
        Assertions.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-last-page"))));
        
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
        Assertions.assertEquals(2, queryRequestEvents.size());
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
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // pump enough results into the queue to trigger two complete pages
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        int pageSize = queryStatus.getQuery().getPagesize();
        
        // test field value pairings
        MultiValueMap<String,String> fieldValues = new LinkedMultiValueMap<>();
        fieldValues.add("LOKI", "ALLIGATOR");
        fieldValues.add("LOKI", "CLASSIC");
        
        // verify that the create event was published
        Assertions.assertEquals(1, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:off

        // add a config object to the query status, which would normally be added by the executor service
        queryStatus.setConfig(new GenericQueryConfiguration());
        queryStorageCache.updateQueryStatus(queryStatus);

        for (int page = 1; page <= 2; page++) {
            // NOTE: We have to generate the results in between next calls because the test queue manager does not handle requeueing of unused messages :(
            // @formatter:off
            publishEventsToQueue(
                    queryId,
                    pageSize,
                    fieldValues,
                    "ALL");
            // @formatter:on
            
            // make the next call asynchronously
            Future<ResponseEntity<DefaultEventQueryResponse>> future = nextQuery(authUser, queryId);
            
            // the response should come back right away
            ResponseEntity<DefaultEventQueryResponse> response = future.get();
            
            Assertions.assertEquals(200, response.getStatusCodeValue());
            
            // verify some headers
            Assertions.assertEquals(Integer.toString(page), Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-page-number"))));
            Assertions.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-Partial-Results"))));
            Assertions.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-last-page"))));
            
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
            Assertions.assertEquals(1, queryRequestEvents.size());
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
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // pump enough results into the queue to trigger a complete page
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        int pageSize = queryStatus.getQuery().getPagesize();
        
        // test field value pairings
        MultiValueMap<String,String> fieldValues = new LinkedMultiValueMap<>();
        fieldValues.add("LOKI", "ALLIGATOR");
        fieldValues.add("LOKI", "CLASSIC");
        
        int numEvents = (int) (0.5 * pageSize);
        
        // add a config object to the query status, which would normally be added by the executor service
        queryStatus.setConfig(new GenericQueryConfiguration());
        queryStorageCache.updateQueryStatus(queryStatus);
        
        // @formatter:off
        publishEventsToQueue(
                queryId,
                numEvents,
                fieldValues,
                "ALL");
        // @formatter:on
        
        // make the next call asynchronously
        Future<ResponseEntity<DefaultEventQueryResponse>> nextFuture = nextQuery(authUser, queryId);
        
        // make sure all events were consumed before canceling
        while (queryQueueManager.getNumResultsRemaining(queryId) != 0) {
            Thread.sleep(100);
        }
        
        // cancel the query so that it returns partial results
        Future<ResponseEntity<VoidResponse>> cancelFuture = cancelQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assertions.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        // the response should come back right away
        ResponseEntity<DefaultEventQueryResponse> nextResponse = nextFuture.get();
        
        Assertions.assertEquals(200, nextResponse.getStatusCodeValue());
        
        // verify some headers
        Assertions.assertEquals("1", Iterables.getOnlyElement(Objects.requireNonNull(nextResponse.getHeaders().get("X-query-page-number"))));
        Assertions.assertEquals("true", Iterables.getOnlyElement(Objects.requireNonNull(nextResponse.getHeaders().get("X-Partial-Results"))));
        Assertions.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(nextResponse.getHeaders().get("X-query-last-page"))));
        
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
    public void testNextSuccess_maxResults() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // pump enough results into the queue to trigger two complete pages
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        int pageSize = queryStatus.getQuery().getPagesize();
        
        // test field value pairings
        MultiValueMap<String,String> fieldValues = new LinkedMultiValueMap<>();
        fieldValues.add("LOKI", "ALLIGATOR");
        fieldValues.add("LOKI", "CLASSIC");
        
        // verify that the create event was published
        Assertions.assertEquals(1, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:on
        
        // add a config object to the query status, which would normally be added by the executor service
        queryStatus.setConfig(new GenericQueryConfiguration());
        queryStorageCache.updateQueryStatus(queryStatus);
        
        for (int page = 1; page <= 4; page++) {
            // NOTE: We have to generate the results in between next calls because the test queue manager does not handle requeueing of unused messages :(
            // @formatter:off
            publishEventsToQueue(
                    queryId,
                    pageSize,
                    fieldValues,
                    "ALL");
            // @formatter:on
            
            // make the next call asynchronously
            Future<ResponseEntity<DefaultEventQueryResponse>> future = nextQuery(authUser, queryId);
            
            // the response should come back right away
            ResponseEntity<DefaultEventQueryResponse> response = future.get();
            
            if (page != 4) {
                Assertions.assertEquals(200, response.getStatusCodeValue());
            } else {
                Assertions.assertEquals(204, response.getStatusCodeValue());
            }
            
            if (page != 4) {
                // verify some headers
                Assertions.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-Partial-Results"))));
                Assertions.assertEquals(Integer.toString(page),
                                Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-page-number"))));
                Assertions.assertEquals("false", Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-last-page"))));
                
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
                Assertions.assertEquals(1, queryRequestEvents.size());
                // @formatter:off
                assertQueryRequestEvent(
                        "executor-unassigned:**",
                        QueryRequest.Method.NEXT,
                        queryId,
                        queryRequestEvents.removeLast());
                // @formatter:on
            } else {
                Assertions.assertNull(response.getBody());
                
                // verify that the next and close events were published
                Assertions.assertEquals(2, queryRequestEvents.size());
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
        Future<ResponseEntity<DefaultEventQueryResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
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
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // mark the task states as complete, and mark task creation as complete to make it appear that the executor has finished
        TaskStates taskStates = queryStorageCache.getTaskStates(queryId);
        for (int i = 0; i < taskStates.getNextTaskId(); i++) {
            taskStates.setState(i, TaskStates.TASK_STATE.COMPLETED);
        }
        queryStorageCache.updateTaskStates(taskStates);
        queryStorageCache.updateCreateStage(queryId, QueryStatus.CREATE_STAGE.RESULTS);
        
        // make the next call asynchronously
        Future<ResponseEntity<DefaultEventQueryResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
        Assertions.assertEquals(204, response.getStatusCodeValue());
        Assertions.assertNull(response.getBody());
        
        // verify that the next event was published
        Assertions.assertEquals(3, queryRequestEvents.size());
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
        DatawaveUserDetails authUser = createUserDetails();
        
        String queryId = UUID.randomUUID().toString();
        
        // make the next call asynchronously
        Future<ResponseEntity<DefaultEventQueryResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
        Assertions.assertEquals(404, response.getStatusCodeValue());
        
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
    public void testNextFailure_queryNotRunning() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // cancel the query so that it returns partial results
        Future<ResponseEntity<VoidResponse>> cancelFuture = cancelQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assertions.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        // make the next call asynchronously
        Future<ResponseEntity<DefaultEventQueryResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Cannot call next on a query that is not running",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
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
    public void testNextFailure_ownershipFailure() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        DatawaveUserDetails altAuthUser = createAltUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // make the next call as an alternate user asynchronously
        Future<ResponseEntity<DefaultEventQueryResponse>> future = nextQuery(altAuthUser, queryId);
        
        // the response should come back right away
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
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
    
    @DirtiesContext
    @Test
    public void testNextFailure_timeout() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // override the call timeout for this test
        queryProperties.getExpiration().setCallTimeout(0);
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // make the next call asynchronously
        Future<ResponseEntity<DefaultEventQueryResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back after the configured timeout (5 seconds)
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
        Assertions.assertEquals(500, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Query timed out. " + queryId + " timed out.",
                "Exception with no cause caught",
                "500-27",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that the next events were published
        Assertions.assertEquals(2, queryRequestEvents.size());
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
        DatawaveUserDetails authUser = createUserDetails();
        
        // define a valid query
        String queryId = defineQuery(authUser, createParams());
        
        // make the next call
        Future<ResponseEntity<DefaultEventQueryResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Cannot call next on a query that is not running",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that no events were published
        Assertions.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testNextFailure_nextOnClosed() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // close the query
        Future<ResponseEntity<VoidResponse>> closeFuture = closeQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> closeResponse = closeFuture.get();
        
        Assertions.assertEquals(200, closeResponse.getStatusCodeValue());
        
        // make the next call asynchronously
        Future<ResponseEntity<DefaultEventQueryResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Cannot call next on a query that is not running",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
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
                QueryRequest.Method.CLOSE,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testNextFailure_nextOnCanceled() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        // cancel the query
        Future<ResponseEntity<VoidResponse>> cancelFuture = cancelQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<VoidResponse> cancelResponse = cancelFuture.get();
        
        Assertions.assertEquals(200, cancelResponse.getStatusCodeValue());
        
        // make the next call asynchronously
        Future<ResponseEntity<DefaultEventQueryResponse>> future = nextQuery(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Cannot call next on a query that is not running",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
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
}
