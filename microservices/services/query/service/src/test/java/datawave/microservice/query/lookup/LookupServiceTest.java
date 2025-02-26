package datawave.microservice.query.lookup;

import static datawave.microservice.query.QueryParameters.QUERY_MAX_CONCURRENT_TASKS;
import static datawave.microservice.query.QueryParameters.QUERY_MAX_RESULTS_OVERRIDE;
import static datawave.microservice.query.lookup.LookupService.LOOKUP_UUID_PAIRS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;

import com.google.common.collect.Iterables;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryKey;
import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.AbstractQueryServiceTest;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.messaging.QueryResultsPublisher;
import datawave.microservice.query.messaging.Result;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.Metadata;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.VoidResponse;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryStarterDefaults", "QueryStarterOverrides", "QueryServiceTest", RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE})
public class LookupServiceTest extends AbstractQueryServiceTest {
    
    @Autowired
    public LookupProperties lookupProperties;
    
    @Test
    public void testLookupUUIDSuccess() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        MultiValueMap<String,String> uuidParams = createUUIDParams();
        
        String uuidType = "PAGE_TITLE";
        String uuid = "anarchy";
        
        Future<ResponseEntity<DefaultEventQueryResponse>> future = lookupUUID(authUser, uuidParams, uuidType, uuid);
        
        String queryId = null;
        
        // get the lookup query id
        QueryStatus queryStatus = null;
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < TEST_WAIT_TIME_MILLIS && queryId == null) {
            List<QueryStatus> queryStatuses = queryStorageCache.getQueryStatus();
            if (queryStatuses.size() > 0) {
                queryStatus = queryStatuses.get(0);
                queryId = queryStatuses.get(0).getQueryKey().getQueryId();
            } else {
                Thread.sleep(500);
            }
        }
        
        // pump enough results into the queue to trigger a complete page
        int pageSize = queryStatus.getQuery().getPagesize();
        
        // test field value pairings
        MultiValueMap<String,String> fieldValues = new LinkedMultiValueMap<>();
        fieldValues.add(uuidType, uuid);
        
        // add a config object to the query status, which would normally be added by the executor service
        queryStatus.setConfig(new GenericQueryConfiguration());
        queryStorageCache.updateQueryStatus(queryStatus);
        
        // @formatter:off
        publishEventsToQueue(
                queryId,
                pageSize,
                fieldValues,
                "ALL");
        // @formatter:on
        
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        // verify some headers
        Assertions.assertEquals("1",
                        Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-page-number"), response.getHeaders().toString())));
        Assertions.assertEquals("false",
                        Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-Partial-Results"), response.getHeaders().toString())));
        Assertions.assertEquals("false",
                        Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-last-page"), response.getHeaders().toString())));
        
        DefaultEventQueryResponse queryResponse = (DefaultEventQueryResponse) response.getBody();
        
        // verify the query response
        // @formatter:off
        assertQueryResponse(
                queryId,
                "LuceneUUIDEventQuery",
                1,
                false,
                Long.parseLong(Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-OperationTimeInMS"), response.getHeaders().toString()))),
                1,
                Collections.singletonList(uuidType),
                pageSize,
                Objects.requireNonNull(queryResponse));
        // @formatter:on
        
        // validate one of the events
        DefaultEvent event = (DefaultEvent) queryResponse.getEvents().get(0);
        // @formatter:off
        assertDefaultEvent(
                Collections.singletonList(uuidType),
                Collections.singletonList(uuid),
                event);
        // @formatter:on
        
        // verify that the correct events were published
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
    public void testBatchLookupUUIDSuccess() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        MultiValueMap<String,String> uuidParams = createUUIDParams();
        uuidParams.add(LOOKUP_UUID_PAIRS, "PAGE_TITLE:anarchy OR PAGE_TITLE:accessiblecomputing");
        
        Future<ResponseEntity<DefaultEventQueryResponse>> future = batchLookupUUID(authUser, uuidParams);
        
        String queryId = null;
        
        // get the lookup query id
        QueryStatus queryStatus = null;
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < TEST_WAIT_TIME_MILLIS && queryId == null) {
            List<QueryStatus> queryStatuses = queryStorageCache.getQueryStatus();
            if (queryStatuses.size() > 0) {
                queryStatus = queryStatuses.get(0);
                queryId = queryStatuses.get(0).getQueryKey().getQueryId();
            } else {
                Thread.sleep(500);
            }
        }
        
        // pump enough results into the queue to trigger a complete page
        int pageSize = queryStorageCache.getQueryStatus(queryId).getQuery().getPagesize();
        
        // test field value pairings
        MultiValueMap<String,String> fieldValues = new LinkedMultiValueMap<>();
        fieldValues.add("PAGE_TITLE", "anarchy");
        fieldValues.add("PAGE_TITLE", "accessiblecomputing");
        
        // add a config object to the query status, which would normally be added by the executor service
        queryStatus.setConfig(new GenericQueryConfiguration());
        queryStorageCache.updateQueryStatus(queryStatus);
        
        // @formatter:off
        publishEventsToQueue(
                queryId,
                pageSize,
                fieldValues,
                "ALL");
        // @formatter:on
        
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        // verify some headers
        Assertions.assertEquals("1",
                        Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-page-number"), response.getHeaders().toString())));
        Assertions.assertEquals("false",
                        Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-Partial-Results"), response.getHeaders().toString())));
        Assertions.assertEquals("false",
                        Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-last-page"), response.getHeaders().toString())));
        
        DefaultEventQueryResponse queryResponse = (DefaultEventQueryResponse) response.getBody();
        
        // verify the query response
        // @formatter:off
        assertQueryResponse(
                queryId,
                "LuceneUUIDEventQuery",
                1,
                false,
                Long.parseLong(Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-OperationTimeInMS"), response.getHeaders().toString()))),
                1,
                Collections.singletonList("PAGE_TITLE"),
                pageSize,
                Objects.requireNonNull(queryResponse));
        // @formatter:on
        
        // validate one of the events
        DefaultEvent event = (DefaultEvent) queryResponse.getEvents().get(0);
        // @formatter:off
        assertDefaultEvent(
                Arrays.asList("PAGE_TITLE", "PAGE_TITLE"),
                Arrays.asList("anarchy", "accessiblecomputing"),
                event);
        // @formatter:on
        
        // verify that the correct events were published
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
    public void testLookupContentUUIDSuccess() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        MultiValueMap<String,String> uuidParams = createUUIDParams();
        
        String uuidType = "PAGE_TITLE";
        String uuid = "anarchy";
        
        Future<ResponseEntity<DefaultEventQueryResponse>> future = lookupContentUUID(authUser, uuidParams, uuidType, uuid);
        
        String queryId = null;
        
        // get the lookup query id
        QueryStatus queryStatus = null;
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < TEST_WAIT_TIME_MILLIS && queryId == null) {
            List<QueryStatus> queryStatuses = queryStorageCache.getQueryStatus();
            if (queryStatuses.size() > 0) {
                queryStatus = queryStatuses.get(0);
                queryId = queryStatuses.get(0).getQueryKey().getQueryId();
            } else {
                Thread.sleep(500);
            }
        }
        
        // pump enough results into the queue to trigger a complete page
        int pageSize = queryStatus.getQuery().getPagesize();
        
        // test field value pairings
        MultiValueMap<String,String> fieldValues = new LinkedMultiValueMap<>();
        fieldValues.add(uuidType, uuid);
        
        // add a config object to the query status, which would normally be added by the executor service
        queryStatus.setConfig(new GenericQueryConfiguration());
        queryStorageCache.updateQueryStatus(queryStatus);
        
        // @formatter:off
        publishEventsToQueue(
                queryId,
                pageSize,
                fieldValues,
                "ALL");
        // @formatter:on
        
        Set<String> contentQueryIds = null;
        // wait for the initial event query to be closed
        startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < TEST_WAIT_TIME_MILLIS && contentQueryIds == null) {
            final String eventQueryId = queryId;
            List<QueryStatus> queryStatuses = queryStorageCache.getQueryStatus();
            if (queryStatuses.size() == 1 + Math.ceil((double) pageSize / lookupProperties.getBatchLookupLimit())) {
                contentQueryIds = queryStatuses.stream().map(QueryStatus::getQueryKey).map(QueryKey::getQueryId)
                                .filter(contentQueryId -> !contentQueryId.equals(eventQueryId)).collect(Collectors.toSet());
            }
            // add a config object to the query status, which would normally be added by the executor service
            for (QueryStatus status : queryStatuses) {
                if (status.getConfig() == null) {
                    status.setConfig(new GenericQueryConfiguration());
                    queryStorageCache.updateQueryStatus(status);
                }
            }
            Thread.sleep(500);
        }
        
        Assertions.assertNotNull(contentQueryIds);
        for (String contentQueryId : contentQueryIds) {
            MultiValueMap<String,String> contentFieldValues = new LinkedMultiValueMap<>();
            contentFieldValues.add("CONTENT", "look I made you some content!");
            
            // @formatter:off
            publishEventsToQueue(
                    contentQueryId,
                    pageSize,
                    contentFieldValues,
                    "ALL");
            // @formatter:on
        }
        
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        // verify some headers
        Assertions.assertEquals("1",
                        Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-page-number"), response.getHeaders().toString())));
        Assertions.assertEquals("false",
                        Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-Partial-Results"), response.getHeaders().toString())));
        Assertions.assertEquals("false",
                        Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-last-page"), response.getHeaders().toString())));
        
        DefaultEventQueryResponse queryResponse = (DefaultEventQueryResponse) response.getBody();
        
        String responseQueryId = queryResponse.getQueryId();
        
        Assertions.assertTrue(contentQueryIds.contains(responseQueryId));
        
        // verify the query response
        // @formatter:off
        assertContentQueryResponse(
                responseQueryId,
                "ContentQuery",
                1,
                false,
                Long.parseLong(Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-OperationTimeInMS"), response.getHeaders().toString()))),
                pageSize,
                Objects.requireNonNull(queryResponse));
        // @formatter:on
        
        // validate one of the events
        DefaultEvent event = (DefaultEvent) queryResponse.getEvents().get(0);
        // @formatter:off
        assertDefaultEvent(
                Collections.singletonList("CONTENT"),
                Collections.singletonList("look I made you some content!"),
                event);
        // @formatter:on
        
        // verify that the correct events were published
        Assertions.assertEquals(7, queryRequestEvents.size());
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
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                responseQueryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.NEXT,
                responseQueryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.NEXT,
                responseQueryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CLOSE,
                responseQueryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testBatchLookupContentUUIDSuccess() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        MultiValueMap<String,String> uuidParams = createUUIDParams();
        uuidParams.add(LOOKUP_UUID_PAIRS, "PAGE_TITLE:anarchy OR PAGE_TITLE:accessiblecomputing");
        uuidParams.add(QUERY_MAX_RESULTS_OVERRIDE, "10");
        
        Future<ResponseEntity<DefaultEventQueryResponse>> future = batchLookupContentUUID(authUser, uuidParams);
        
        String queryId = null;
        
        // get the lookup query id
        QueryStatus queryStatus = null;
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < TEST_WAIT_TIME_MILLIS && queryId == null) {
            List<QueryStatus> queryStatuses = queryStorageCache.getQueryStatus();
            if (queryStatuses.size() > 0) {
                queryStatus = queryStatuses.get(0);
                queryId = queryStatuses.get(0).getQueryKey().getQueryId();
            } else {
                Thread.sleep(500);
            }
        }
        
        // pump enough results into the queue to trigger a complete page
        int pageSize = queryStatus.getQuery().getPagesize();
        
        // test field value pairings
        MultiValueMap<String,String> fieldValues = new LinkedMultiValueMap<>();
        fieldValues.add("PAGE_TITLE", "anarchy");
        fieldValues.add("PAGE_TITLE", "accessiblecomputing");
        
        // add a config object to the query status, which would normally be added by the executor service
        queryStatus.setConfig(new GenericQueryConfiguration());
        queryStorageCache.updateQueryStatus(queryStatus);
        
        // @formatter:off
        publishEventsToQueue(
                queryId,
                pageSize,
                fieldValues,
                "ALL");
        // @formatter:on
        
        Set<String> contentQueryIds = null;
        // wait for the initial event query to be closed
        startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < TEST_WAIT_TIME_MILLIS && contentQueryIds == null) {
            final String eventQueryId = queryId;
            List<QueryStatus> queryStatuses = queryStorageCache.getQueryStatus();
            if (queryStatuses.size() == 1 + Math.ceil((double) pageSize / lookupProperties.getBatchLookupLimit())) {
                contentQueryIds = queryStatuses.stream().map(QueryStatus::getQueryKey).map(QueryKey::getQueryId)
                                .filter(contentQueryId -> !contentQueryId.equals(eventQueryId)).collect(Collectors.toSet());
            }
            // add a config object to the query status, which would normally be added by the executor service
            for (QueryStatus status : queryStatuses) {
                if (status.getConfig() == null) {
                    status.setConfig(new GenericQueryConfiguration());
                    queryStorageCache.updateQueryStatus(status);
                }
            }
            Thread.sleep(500);
        }
        
        Assertions.assertNotNull(contentQueryIds);
        for (String contentQueryId : contentQueryIds) {
            MultiValueMap<String,String> contentFieldValues = new LinkedMultiValueMap<>();
            contentFieldValues.add("CONTENT", "look I made you some content!");
            
            // @formatter:off
            publishEventsToQueue(
                    contentQueryId,
                    pageSize,
                    contentFieldValues,
                    "ALL");
            // @formatter:on
        }
        
        // wait until each query has read its results, and then close it
        for (String contentQueryId : contentQueryIds) {
            QueryStatus status = queryStorageCache.getQueryStatus(contentQueryId);
            startTime = System.currentTimeMillis();
            while ((System.currentTimeMillis() - startTime) < TEST_WAIT_TIME_MILLIS && status.getNumResultsConsumed() < pageSize) {
                Thread.sleep(500);
                status = queryStorageCache.getQueryStatus(contentQueryId);
            }
        }
        
        ResponseEntity<DefaultEventQueryResponse> response = future.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        // verify some headers
        Assertions.assertEquals("1",
                        Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-page-number"), response.getHeaders().toString())));
        Assertions.assertEquals("false",
                        Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-Partial-Results"), response.getHeaders().toString())));
        Assertions.assertEquals("false",
                        Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-query-last-page"), response.getHeaders().toString())));
        
        DefaultEventQueryResponse queryResponse = (DefaultEventQueryResponse) response.getBody();
        
        String responseQueryId = queryResponse.getQueryId();
        
        Assertions.assertTrue(contentQueryIds.contains(responseQueryId));
        
        // verify the query response
        // @formatter:off
        assertContentQueryResponse(
                responseQueryId,
                "ContentQuery",
                1,
                false,
                Long.parseLong(Iterables.getOnlyElement(Objects.requireNonNull(response.getHeaders().get("X-OperationTimeInMS"), response.getHeaders().toString()))),
                pageSize,
                Objects.requireNonNull(queryResponse));
        // @formatter:on
        
        // validate one of the events
        DefaultEvent event = (DefaultEvent) queryResponse.getEvents().get(0);
        // @formatter:off
        assertDefaultEvent(
                Collections.singletonList("CONTENT"),
                Collections.singletonList("look I made you some content!"),
                event);
        // @formatter:on
        
        // verify that the correct events were published
        Assertions.assertEquals(7, queryRequestEvents.size());
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
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                responseQueryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.NEXT,
                responseQueryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.NEXT,
                responseQueryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CLOSE,
                responseQueryId,
                queryRequestEvents.removeLast());
        // @formatter:on
    }
    
    @Test
    public void testBatchLookupUUIDFailure_noLookupUUIDPairs() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        MultiValueMap<String,String> uuidParams = createUUIDParams();
        
        UriComponents uri = createUri("lookupUUID");
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, uuidParams, null, HttpMethod.POST, uri);
        
        ResponseEntity<VoidResponse> response = jwtRestTemplate.exchange(requestEntity, VoidResponse.class);
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Missing required parameter.",
                "Exception with no cause caught",
                "400-40",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
    }
    
    @Test
    public void testBatchLookupUUIDFailure_mixedQueryLogics() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        MultiValueMap<String,String> uuidParams = createUUIDParams();
        uuidParams.add(LOOKUP_UUID_PAIRS, "PAGE_TITLE:anarchy OR PAGE_NUMBER:accessiblecomputing");
        
        UriComponents uri = createUri("lookupUUID");
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, uuidParams, null, HttpMethod.POST, uri);
        
        ResponseEntity<VoidResponse> response = jwtRestTemplate.exchange(requestEntity, VoidResponse.class);
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Multiple UUID types 'LuceneUUIDEventQuery' and 'EventQuery' not supported within the same lookup request",
                "java.lang.IllegalArgumentException: Multiple UUID types 'LuceneUUIDEventQuery' and 'EventQuery' not supported within the same lookup request",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
    }
    
    @Test
    public void testBatchLookupUUIDFailure_nullUUIDType() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        MultiValueMap<String,String> uuidParams = createUUIDParams();
        uuidParams.add(LOOKUP_UUID_PAIRS, "PAGE:anarchy");
        
        UriComponents uri = createUri("lookupUUID");
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, uuidParams, null, HttpMethod.POST, uri);
        
        ResponseEntity<VoidResponse> response = jwtRestTemplate.exchange(requestEntity, VoidResponse.class);
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Invalid type 'PAGE' for UUID anarchy not supported with the LuceneToJexlUUIDQueryParser",
                "java.lang.IllegalArgumentException: Invalid type 'PAGE' for UUID anarchy not supported with the LuceneToJexlUUIDQueryParser",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
    }
    
    @Test
    public void testBatchLookupUUIDFailure_emptyUUIDFieldValue() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        MultiValueMap<String,String> uuidParams = createUUIDParams();
        uuidParams.add(LOOKUP_UUID_PAIRS, ":anarchy");
        
        UriComponents uri = createUri("lookupUUID");
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, uuidParams, null, HttpMethod.POST, uri);
        
        ResponseEntity<VoidResponse> response = jwtRestTemplate.exchange(requestEntity, VoidResponse.class);
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Empty UUID type or value extracted from uuidPair :anarchy",
                "java.lang.IllegalArgumentException: Empty UUID type or value extracted from uuidPair :anarchy",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
    }
    
    @Test
    public void testBatchLookupUUIDFailure_invalidUUIDPair() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        MultiValueMap<String,String> uuidParams = createUUIDParams();
        uuidParams.add(LOOKUP_UUID_PAIRS, ":");
        
        UriComponents uri = createUri("lookupUUID");
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, uuidParams, null, HttpMethod.POST, uri);
        
        ResponseEntity<VoidResponse> response = jwtRestTemplate.exchange(requestEntity, VoidResponse.class);
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Unable to determine UUID type and value from uuidPair :",
                "java.lang.IllegalArgumentException: Unable to determine UUID type and value from uuidPair :",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
    }
    
    @Test
    public void testBatchLookupUUIDFailure_tooManyTerms() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        MultiValueMap<String,String> uuidParams = createUUIDParams();
        
        StringBuilder lookupUUIDPairs = new StringBuilder();
        for (int i = 0; i < lookupProperties.getBatchLookupLimit() + 1; i++) {
            if (i > 0) {
                lookupUUIDPairs.append(" OR ");
            }
            lookupUUIDPairs.append("PAGE_TITLE:anarchy-").append(i);
        }
        uuidParams.add(LOOKUP_UUID_PAIRS, lookupUUIDPairs.toString());
        
        UriComponents uri = createUri("lookupUUID");
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, uuidParams, null, HttpMethod.POST, uri);
        
        ResponseEntity<VoidResponse> response = jwtRestTemplate.exchange(requestEntity, VoidResponse.class);
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "The " + (lookupProperties.getBatchLookupLimit() + 1) + " specified UUIDs exceed the maximum number of " + lookupProperties.getBatchLookupLimit() + " allowed for a given lookup request",
                "java.lang.IllegalArgumentException: The " + (lookupProperties.getBatchLookupLimit() + 1) + " specified UUIDs exceed the maximum number of " + lookupProperties.getBatchLookupLimit() + " allowed for a given lookup request",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
    }
    
    @Test
    public void testBatchLookupUUIDFailure_nonLookupQueryLogic() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        MultiValueMap<String,String> uuidParams = createUUIDParams();
        uuidParams.add(LOOKUP_UUID_PAIRS, "PAGE_NUMBER:accessiblecomputing");
        
        UriComponents uri = createUri("lookupUUID");
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, uuidParams, null, HttpMethod.POST, uri);
        
        ResponseEntity<VoidResponse> response = jwtRestTemplate.exchange(requestEntity, VoidResponse.class);
        
        Assertions.assertEquals(500, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "Error setting up query. Lookup UUID can only be run with a LookupQueryLogic",
                "Exception with no cause caught",
                "500-66",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
    }
    
    protected MultiValueMap<String,String> createUUIDParams() {
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.set(DefaultQueryParameters.QUERY_NAME, TEST_QUERY_NAME);
        map.set(DefaultQueryParameters.QUERY_AUTHORIZATIONS, TEST_QUERY_AUTHORIZATIONS);
        map.set(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, TEST_VISIBILITY_MARKING);
        map.set(QUERY_MAX_CONCURRENT_TASKS, Integer.toString(1));
        return map;
    }
    
    protected Future<ResponseEntity<DefaultEventQueryResponse>> batchLookupUUID(DatawaveUserDetails authUser, MultiValueMap<String,String> map) {
        UriComponents uri = createUri("lookupUUID");
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, DefaultEventQueryResponse.class));
    }
    
    protected Future<ResponseEntity<DefaultEventQueryResponse>> lookupUUID(DatawaveUserDetails authUser, MultiValueMap<String,String> map, String uuidType,
                    String uuid) {
        UriComponents uri = createUri("lookupUUID/" + uuidType + "/" + uuid);
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.GET, uri);
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, DefaultEventQueryResponse.class));
    }
    
    protected Future<ResponseEntity<DefaultEventQueryResponse>> batchLookupContentUUID(DatawaveUserDetails authUser, MultiValueMap<String,String> map) {
        UriComponents uri = createUri("lookupContentUUID");
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, DefaultEventQueryResponse.class));
    }
    
    protected Future<ResponseEntity<DefaultEventQueryResponse>> lookupContentUUID(DatawaveUserDetails authUser, MultiValueMap<String,String> map,
                    String uuidType, String uuid) {
        UriComponents uri = createUri("lookupContentUUID/" + uuidType + "/" + uuid);
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.GET, uri);
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, DefaultEventQueryResponse.class));
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
            
            Metadata metadata = new Metadata();
            // tonight i'm gonna party like it's
            metadata.setRow("19991231_0");
            metadata.setDataType("prince");
            metadata.setInternalId(UUID.randomUUID().toString());
            event.setMetadata(metadata);
            publisher.publish(new Result(Integer.toString(resultId), event));
        }
    }
    
    protected void assertContentQueryResponse(String queryId, String logicName, long pageNumber, boolean partialResults, long operationTimeInMS, int numEvents,
                    DefaultEventQueryResponse queryResponse) {
        Assertions.assertEquals(queryId, queryResponse.getQueryId());
        Assertions.assertEquals(logicName, queryResponse.getLogicName());
        Assertions.assertEquals(pageNumber, queryResponse.getPageNumber());
        Assertions.assertEquals(partialResults, queryResponse.isPartialResults());
        Assertions.assertEquals(operationTimeInMS, queryResponse.getOperationTimeMS());
        Assertions.assertEquals(numEvents, queryResponse.getEvents().size());
    }
}
