package datawave.microservice.query;

import static datawave.microservice.query.QueryImpl.BEGIN_DATE;
import static datawave.microservice.query.QueryParameters.QUERY_MAX_CONCURRENT_TASKS;
import static datawave.microservice.query.QueryParameters.QUERY_MAX_RESULTS_OVERRIDE;
import static datawave.microservice.query.QueryParameters.QUERY_PAGESIZE;
import static datawave.webservice.common.audit.AuditParameters.QUERY_STRING;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;

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
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.GenericResponse;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryStarterDefaults", "QueryStarterOverrides", "QueryServiceTest", RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE})
@ContextConfiguration(classes = {QueryService.class})
public class QueryServiceCreateTest extends AbstractQueryServiceTest {
    
    @Test
    public void testCreateSuccess() throws ParseException, IOException {
        DatawaveUserDetails authUser = createUserDetails();
        UriComponents uri = createUri("EventQuery/create");
        MultiValueMap<String,String> map = createParams();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        // setup a mock audit service
        auditSentSetup();
        
        long currentTimeMillis = System.currentTimeMillis();
        ResponseEntity<GenericResponse> resp = jwtRestTemplate.exchange(requestEntity, GenericResponse.class);
        // @formatter:off
        GenericResponse<String> genericResponse = assertGenericResponse(
                true,
                HttpStatus.Series.SUCCESSFUL,
                resp);
        // @formatter:on
        
        // verify that a query id was returned
        String queryId = genericResponse.getResult();
        Assertions.assertNotNull(queryId);
        
        // verify that the create event was published
        Assertions.assertEquals(1, queryRequestEvents.size());
        // @formatter:off
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.CREATE,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:on
        
        // verify that query status was created correctly
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
        
        // verify that they query was created correctly
        Query query = queryStatus.getQuery();
        
        // @formatter:off
        assertQuery(
                TEST_QUERY_STRING,
                TEST_QUERY_NAME,
                TEST_QUERY_AUTHORIZATIONS,
                TEST_QUERY_BEGIN,
                TEST_QUERY_END,
                TEST_VISIBILITY_MARKING,
                query);
        // @formatter:on
        
        // verify that an audit message was sent and the the audit id matches the query id
        assertAuditSent(queryId);
        
        // verify that query tasks were created
        assertTasksCreated(queryId);
    }
    
    @Test
    public void testCreateFailure_paramValidation() {
        DatawaveUserDetails authUser = createUserDetails();
        UriComponents uri = createUri("EventQuery/create");
        MultiValueMap<String,String> map = createParams();
        
        // remove the query param to induce a parameter validation failure
        map.remove(QUERY_STRING);
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        // setup a mock audit service
        auditNotSentSetup();
        
        ResponseEntity<GenericResponse> resp = jwtRestTemplate.exchange(requestEntity, GenericResponse.class);
        
        // @formatter:off
        BaseResponse baseResponse = assertBaseResponse(
                false,
                HttpStatus.Series.CLIENT_ERROR,
                resp);
        // @formatter:on
        
        // verify that there is no result
        Assertions.assertFalse(baseResponse.getHasResults());
        
        // verify that an exception was returned
        Assertions.assertEquals(1, baseResponse.getExceptions().size());
        
        QueryExceptionType queryException = baseResponse.getExceptions().get(0);
        // @formatter:off
        assertQueryException(
                "Missing one or more required QueryParameters",
                "java.lang.IllegalArgumentException: Missing one or more required QueryParameters",
                "400-1",
                queryException);
        // @formatter:on
        
        // verify that there are no query statuses
        Assertions.assertTrue(queryStorageCache.getQueryStatus().isEmpty());
        
        // verify that no audit message was sent
        assertAuditNotSent();
    }
    
    @Test
    public void testCreateFailure_authValidation() {
        DatawaveUserDetails authUser = createUserDetails(Collections.singleton("AuthorizedUser"), Collections.emptyList());
        UriComponents uri = createUri("EventQuery/create");
        MultiValueMap<String,String> map = createParams();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        // setup a mock audit service
        auditSentSetup();
        
        ResponseEntity<GenericResponse> resp = jwtRestTemplate.exchange(requestEntity, GenericResponse.class);
        
        // @formatter:off
        BaseResponse baseResponse = assertBaseResponse(
                false,
                HttpStatus.Series.CLIENT_ERROR,
                resp);
        // @formatter:on
        
        // verify that there is no result
        Assertions.assertFalse(baseResponse.getHasResults());
        
        // verify that an exception was returned
        Assertions.assertEquals(1, baseResponse.getExceptions().size());
        
        QueryExceptionType queryException = baseResponse.getExceptions().get(0);
        // @formatter:off
        assertQueryException(
                "java.lang.IllegalArgumentException: User requested authorizations that they don't have. Missing: [ALL], Requested: [ALL], User: []",
                "datawave.security.authorization.AuthorizationException: java.lang.IllegalArgumentException: User requested authorizations that they don't have. Missing: [ALL], Requested: [ALL], User: []",
                "400-1",
                queryException);
        // @formatter:on
        
        // verify that there are no query statuses
        Assertions.assertTrue(queryStorageCache.getQueryStatus().isEmpty());
        
        // verify that no audit message was sent
        assertAuditSent(null);
    }
    
    @Test
    public void testCreateFailure_queryLogicValidation() {
        DatawaveUserDetails authUser = createUserDetails();
        UriComponents uri = createUri("EventQuery/create");
        MultiValueMap<String,String> map = createParams();
        
        // remove the beginDate param to induce a query logic validation failure
        map.remove(BEGIN_DATE);
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        // setup a mock audit service
        auditNotSentSetup();
        
        ResponseEntity<GenericResponse> resp = jwtRestTemplate.exchange(requestEntity, GenericResponse.class);
        
        // @formatter:off
        BaseResponse baseResponse = assertBaseResponse(
                false,
                HttpStatus.Series.CLIENT_ERROR,
                resp);
        // @formatter:on
        
        // verify that there is no result
        Assertions.assertFalse(baseResponse.getHasResults());
        
        // verify that an exception was returned
        Assertions.assertEquals(1, baseResponse.getExceptions().size());
        
        QueryExceptionType queryException = baseResponse.getExceptions().get(0);
        // @formatter:off
        assertQueryException(
                "Required parameter begin not found",
                "java.lang.IllegalArgumentException: Required parameter begin not found",
                "400-1",
                queryException);
        // @formatter:on
        
        // verify that there are no query statuses
        Assertions.assertTrue(queryStorageCache.getQueryStatus().isEmpty());
        
        // verify that no audit message was sent
        assertAuditNotSent();
    }
    
    @Test
    public void testCreateFailure_maxPageSize() {
        DatawaveUserDetails authUser = createUserDetails(Arrays.asList("AuthorizedUser", queryProperties.getPrivilegedRole()), null);
        UriComponents uri = createUri("EventQuery/create");
        MultiValueMap<String,String> map = createParams();
        
        // set an invalid page size override
        map.set(QUERY_PAGESIZE, Integer.toString(Integer.MAX_VALUE));
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        // setup a mock audit service
        auditNotSentSetup();
        
        ResponseEntity<GenericResponse> resp = jwtRestTemplate.exchange(requestEntity, GenericResponse.class);
        
        // @formatter:off
        BaseResponse baseResponse = assertBaseResponse(
                false,
                HttpStatus.Series.CLIENT_ERROR,
                resp);
        // @formatter:on
        
        // verify that there is no result
        Assertions.assertFalse(baseResponse.getHasResults());
        
        // verify that an exception was returned
        Assertions.assertEquals(1, baseResponse.getExceptions().size());
        
        QueryExceptionType queryException = baseResponse.getExceptions().get(0);
        // @formatter:off
        assertQueryException(
                "Page size is larger than configured max. Max = 10,000.",
                "Exception with no cause caught",
                "400-6",
                queryException);
        // @formatter:on
        
        // verify that there are no query statuses
        Assertions.assertTrue(queryStorageCache.getQueryStatus().isEmpty());
        
        // verify that no audit message was sent
        assertAuditNotSent();
    }
    
    @Test
    public void testCreateFailure_maxResultsOverride() {
        DatawaveUserDetails authUser = createUserDetails();
        UriComponents uri = createUri("EventQuery/create");
        MultiValueMap<String,String> map = createParams();
        
        // set an invalid max results override
        map.set(QUERY_MAX_RESULTS_OVERRIDE, Long.toString(Long.MAX_VALUE));
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        // setup a mock audit service
        auditNotSentSetup();
        
        ResponseEntity<GenericResponse> resp = jwtRestTemplate.exchange(requestEntity, GenericResponse.class);
        
        // @formatter:off
        BaseResponse baseResponse = assertBaseResponse(
                false,
                HttpStatus.Series.CLIENT_ERROR,
                resp);
        // @formatter:on
        
        // verify that there is no result
        Assertions.assertFalse(baseResponse.getHasResults());
        
        // verify that an exception was returned
        Assertions.assertEquals(1, baseResponse.getExceptions().size());
        
        QueryExceptionType queryException = baseResponse.getExceptions().get(0);
        // @formatter:off
        assertQueryException(
                "Invalid max results override value. Max = 369.",
                "Exception with no cause caught",
                "400-43",
                queryException);
        // @formatter:on
        
        // verify that there are no query statuses
        Assertions.assertTrue(queryStorageCache.getQueryStatus().isEmpty());
        
        // verify that no audit message was sent
        assertAuditNotSent();
    }
    
    @Test
    public void testCreateFailure_maxConcurrentTasksOverride() {
        DatawaveUserDetails authUser = createUserDetails();
        UriComponents uri = createUri("EventQuery/create");
        MultiValueMap<String,String> map = createParams();
        
        // add an invalid max results override
        map.set(QUERY_MAX_CONCURRENT_TASKS, Integer.toString(Integer.MAX_VALUE));
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        // setup a mock audit service
        auditNotSentSetup();
        
        ResponseEntity<GenericResponse> resp = jwtRestTemplate.exchange(requestEntity, GenericResponse.class);
        
        // @formatter:off
        BaseResponse baseResponse = assertBaseResponse(
                false,
                HttpStatus.Series.CLIENT_ERROR,
                resp);
        // @formatter:on
        
        // verify that there is no result
        Assertions.assertFalse(baseResponse.getHasResults());
        
        // verify that an exception was returned
        Assertions.assertEquals(1, baseResponse.getExceptions().size());
        
        QueryExceptionType queryException = baseResponse.getExceptions().get(0);
        // @formatter:off
        assertQueryException(
                "Invalid max concurrent tasks override value. Max = 10.",
                "Exception with no cause caught",
                "400-44",
                queryException);
        // @formatter:on
        
        // verify that there are no query statuses
        Assertions.assertTrue(queryStorageCache.getQueryStatus().isEmpty());
        
        // verify that no audit message was sent
        assertAuditNotSent();
    }
    
    @Test
    public void testCreateFailure_roleValidation() {
        // create a user without the required role
        DatawaveUserDetails authUser = createUserDetails(Collections.emptyList(), Collections.singletonList("ALL"));
        UriComponents uri = createUri("EventQuery/create");
        MultiValueMap<String,String> map = createParams();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        // setup a mock audit service
        auditNotSentSetup();
        
        ResponseEntity<GenericResponse> resp = jwtRestTemplate.exchange(requestEntity, GenericResponse.class);
        
        // @formatter:off
        BaseResponse baseResponse = assertBaseResponse(
                false,
                HttpStatus.Series.CLIENT_ERROR,
                resp);
        // @formatter:on
        
        // verify that there is no result
        Assertions.assertFalse(baseResponse.getHasResults());
        
        // verify that an exception was returned
        Assertions.assertEquals(1, baseResponse.getExceptions().size());
        
        QueryExceptionType queryException = baseResponse.getExceptions().get(0);
        // @formatter:off
        assertQueryException(
                "User does not have the required roles.",
                "datawave.webservice.query.exception.UnauthorizedQueryException: User does not have the required roles.",
                "400-5",
                queryException);
        // @formatter:on
        
        // verify that there are no query statuses
        Assertions.assertTrue(queryStorageCache.getQueryStatus().isEmpty());
        
        // verify that no audit message was sent
        assertAuditNotSent();
    }
    
    @Test
    public void testCreateFailure_markingValidation() {
        DatawaveUserDetails authUser = createUserDetails();
        UriComponents uri = createUri("EventQuery/create");
        MultiValueMap<String,String> map = createParams();
        
        // remove the column visibility param to induce a security marking validation failure
        map.remove(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING);
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        // setup a mock audit service
        auditNotSentSetup();
        
        ResponseEntity<GenericResponse> resp = jwtRestTemplate.exchange(requestEntity, GenericResponse.class);
        
        // @formatter:off
        BaseResponse baseResponse = assertBaseResponse(
                false,
                HttpStatus.Series.CLIENT_ERROR,
                resp);
        // @formatter:on
        
        // verify that there is no result
        Assertions.assertFalse(baseResponse.getHasResults());
        
        // verify that an exception was returned
        Assertions.assertEquals(1, baseResponse.getExceptions().size());
        
        QueryExceptionType queryException = baseResponse.getExceptions().get(0);
        // @formatter:off
        assertQueryException(
                "Required parameter columnVisibility not found",
                "java.lang.IllegalArgumentException: Required parameter columnVisibility not found",
                "400-4",
                queryException);
        // @formatter:on
        
        // verify that there are no query statuses
        Assertions.assertTrue(queryStorageCache.getQueryStatus().isEmpty());
        
        // verify that no audit message was sent
        assertAuditNotSent();
    }
}
