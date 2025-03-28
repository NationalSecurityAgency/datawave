package datawave.microservice.query;

import static datawave.microservice.query.QueryImpl.BEGIN_DATE;
import static datawave.microservice.query.QueryImpl.END_DATE;
import static datawave.microservice.query.QueryImpl.PAGESIZE;
import static datawave.microservice.query.QueryImpl.QUERY;
import static datawave.microservice.query.QueryParameters.QUERY_LOGIC_NAME;
import static datawave.webservice.common.audit.AuditParameters.QUERY_AUTHORIZATIONS;
import static datawave.webservice.common.audit.AuditParameters.QUERY_STRING;

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
public class QueryServiceUpdateTest extends AbstractQueryServiceTest {
    
    @Test
    public void testUpdateSuccess_updateOnDefined() throws Exception {
        DatawaveUserDetails authUser = createUserDetails(null, Arrays.asList("ALL", "NONE"));
        
        // define a valid query
        String queryId = defineQuery(authUser, createParams());
        
        String newQuery = "SOME_OTHER_FIELD:SOME_OTHER_VALUE";
        String newAuths = "ALL,NONE";
        String newBegin = "20100101 000000.000";
        String newEnd = "20600101 000000.000";
        String newLogic = "AltEventQuery";
        int newPageSize = 100;
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        updateParams.set(QUERY, newQuery);
        updateParams.set(QUERY_AUTHORIZATIONS, newAuths);
        updateParams.set(BEGIN_DATE, newBegin);
        updateParams.set(END_DATE, newEnd);
        updateParams.set(QUERY_LOGIC_NAME, newLogic);
        updateParams.set(PAGESIZE, Integer.toString(newPageSize));
        
        // update the query
        Future<ResponseEntity<GenericResponse>> updateFuture = updateQuery(authUser, queryId, updateParams);
        
        // the response should come back right away
        ResponseEntity<GenericResponse> response = updateFuture.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        // make sure the query was updated
        Assertions.assertEquals(newQuery, queryStatus.getQuery().getQuery());
        Assertions.assertEquals(newAuths, queryStatus.getQuery().getQueryAuthorizations());
        Assertions.assertEquals(newBegin, DefaultQueryParameters.formatDate(queryStatus.getQuery().getBeginDate()));
        Assertions.assertEquals(newEnd, DefaultQueryParameters.formatDate(queryStatus.getQuery().getEndDate()));
        Assertions.assertEquals(newLogic, queryStatus.getQuery().getQueryLogicName());
        Assertions.assertEquals(newPageSize, queryStatus.getQuery().getPagesize());
        
        // verify that no events were published
        Assertions.assertEquals(0, queryRequestEvents.size());
    }
    
    @Test
    public void testUpdateSuccess_updateOnCreated() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        int newPageSize = 100;
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        updateParams.set(PAGESIZE, Integer.toString(newPageSize));
        
        // update the query
        Future<ResponseEntity<GenericResponse>> updateFuture = updateQuery(authUser, queryId, updateParams);
        
        // the response should come back right away
        ResponseEntity<GenericResponse> response = updateFuture.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        // make sure the query was updated
        Assertions.assertEquals(newPageSize, queryStatus.getQuery().getPagesize());
        
        // verify that events were published
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
    public void testUpdateFailure_unsafeParamUpdateQuery() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        String newQuery = "SOME_OTHER_FIELD:SOME_OTHER_VALUE";
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        updateParams.set(QUERY, newQuery);
        
        // update the query
        UriComponents uri = createUri(queryId + "/update");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, updateParams, null, HttpMethod.PUT, uri);
        
        // make the update call asynchronously
        Future<ResponseEntity<VoidResponse>> updateFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = updateFuture.get();
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        // make sure the query was not updated
        Assertions.assertEquals(TEST_QUERY_STRING, queryStatus.getQuery().getQuery());
        
        // @formatter:off
        assertQueryException(
                "Cannot update the following parameters for a running query: query",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that events were published
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
    public void testUpdateFailure_unsafeParamUpdateDate() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        String newBegin = "20100101 000000.000";
        String newEnd = "20600101 000000.000";
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        updateParams.set(BEGIN_DATE, newBegin);
        updateParams.set(END_DATE, newEnd);
        
        // update the query
        UriComponents uri = createUri(queryId + "/update");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, updateParams, null, HttpMethod.PUT, uri);
        
        // make the update call asynchronously
        Future<ResponseEntity<VoidResponse>> updateFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = updateFuture.get();
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        // make sure the query was not updated
        Assertions.assertEquals(TEST_QUERY_BEGIN, DefaultQueryParameters.formatDate(queryStatus.getQuery().getBeginDate()));
        Assertions.assertEquals(TEST_QUERY_END, DefaultQueryParameters.formatDate(queryStatus.getQuery().getEndDate()));
        
        // @formatter:off
        assertQueryException(
                "Cannot update the following parameters for a running query: begin, end",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that events were published
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
    public void testUpdateFailure_unsafeParamUpdateLogic() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        String newLogic = "AltEventQuery";
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        updateParams.set(QUERY_LOGIC_NAME, newLogic);
        
        // update the query
        UriComponents uri = createUri(queryId + "/update");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, updateParams, null, HttpMethod.PUT, uri);
        
        // make the update call asynchronously
        Future<ResponseEntity<VoidResponse>> updateFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = updateFuture.get();
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        // make sure the query was not updated
        Assertions.assertEquals("EventQuery", queryStatus.getQuery().getQueryLogicName());
        
        // @formatter:off
        assertQueryException(
                "Cannot update the following parameters for a running query: logicName",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that events were published
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
    public void testUpdateFailure_unsafeParamUpdateAuths() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        String newAuths = "ALL,NONE";
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        updateParams.set(QUERY_AUTHORIZATIONS, newAuths);
        
        // update the query
        UriComponents uri = createUri(queryId + "/update");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, updateParams, null, HttpMethod.PUT, uri);
        
        // make the update call asynchronously
        Future<ResponseEntity<VoidResponse>> updateFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = updateFuture.get();
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        
        // make sure the query was not updated
        Assertions.assertEquals(TEST_QUERY_AUTHORIZATIONS, queryStatus.getQuery().getQueryAuthorizations());
        
        // @formatter:off
        assertQueryException(
                "Cannot update the following parameters for a running query: auths",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that events were published
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
    public void testUpdateFailure_nullParams() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        // create a valid query
        String queryId = createQuery(authUser, createParams());
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        
        // update the query
        UriComponents uri = createUri(queryId + "/update");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, updateParams, null, HttpMethod.PUT, uri);
        
        // make the update call asynchronously
        Future<ResponseEntity<VoidResponse>> updateFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = updateFuture.get();
        
        Assertions.assertEquals(400, response.getStatusCodeValue());
        
        // @formatter:off
        assertQueryException(
                "No parameters specified for update.",
                "Exception with no cause caught",
                "400-1",
                Iterables.getOnlyElement(response.getBody().getExceptions()));
        // @formatter:on
        
        // verify that events were published
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
    public void testUpdateFailure_queryNotFound() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        
        String queryId = UUID.randomUUID().toString();
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        updateParams.set(QUERY_STRING, TEST_QUERY_STRING);
        
        // update the query
        UriComponents uri = createUri(queryId + "/update");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, updateParams, null, HttpMethod.PUT, uri);
        
        // make the update call asynchronously
        Future<ResponseEntity<VoidResponse>> updateFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = updateFuture.get();
        
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
    public void testUpdateFailure_ownershipFailure() throws Exception {
        DatawaveUserDetails authUser = createUserDetails();
        DatawaveUserDetails altAuthUser = createAltUserDetails();
        
        // define a valid query
        String queryId = defineQuery(authUser, createParams());
        
        String newQuery = "SOME_OTHER_FIELD:SOME_OTHER_VALUE";
        
        MultiValueMap<String,String> updateParams = new LinkedMultiValueMap<>();
        updateParams.set(QUERY, newQuery);
        
        // update the query
        UriComponents uri = createUri(queryId + "/update");
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(altAuthUser, updateParams, null, HttpMethod.PUT, uri);
        
        // make the update call asynchronously
        Future<ResponseEntity<VoidResponse>> updateFuture = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, VoidResponse.class));
        
        // the response should come back right away
        ResponseEntity<VoidResponse> response = updateFuture.get();
        
        Assertions.assertEquals(401, response.getStatusCodeValue());
        
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
