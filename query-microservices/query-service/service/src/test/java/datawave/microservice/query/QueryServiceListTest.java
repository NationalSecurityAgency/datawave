package datawave.microservice.query;

import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.util.ProxiedEntityUtils;
import datawave.webservice.query.Query;
import datawave.webservice.query.result.logic.QueryLogicDescription;
import datawave.webservice.result.QueryImplListResponse;
import datawave.webservice.result.QueryLogicResponse;
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
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static datawave.microservice.query.QueryParameters.QUERY_NAME;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryStarterDefaults", "QueryStarterOverrides", "QueryServiceTest", RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE})
public class QueryServiceListTest extends AbstractQueryServiceTest {
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void teardown() throws Exception {
        super.teardown();
    }
    
    @Test
    public void testListSuccess() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        ProxiedUserDetails altAuthUser = createAltUserDetails();
        
        // define a bunch of queries as the original user
        List<String> queryIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String queryId = createQuery(authUser, createParams());
            mockServer.reset();
            
            queryIds.add(queryId);
        }
        
        // define a bunch of queries as the alternate user
        List<String> altQueryIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String queryId = defineQuery(altAuthUser, createParams());
            mockServer.reset();
            
            altQueryIds.add(queryId);
        }
        
        // list queries as the original user
        Future<ResponseEntity<QueryImplListResponse>> listFuture = listQueries(authUser, null, null);
        
        // this should return immediately
        ResponseEntity<QueryImplListResponse> listResponse = listFuture.get();
        
        Assert.assertEquals(200, listResponse.getStatusCodeValue());
        
        QueryImplListResponse result = listResponse.getBody();
        
        Assert.assertEquals(5, result.getNumResults());
        
        List<String> actualQueryIds = result.getQuery().stream().map(Query::getId).map(UUID::toString).collect(Collectors.toList());
        
        Collections.sort(queryIds);
        Collections.sort(actualQueryIds);
        
        Assert.assertEquals(queryIds, actualQueryIds);
    }
    
    @Test
    public void testListSuccess_filterOnQueryId() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // define a bunch of queries as the original user
        List<String> queryIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String queryId = createQuery(authUser, createParams());
            mockServer.reset();
            
            queryIds.add(queryId);
        }
        
        // list queries
        Future<ResponseEntity<QueryImplListResponse>> listFuture = listQueries(authUser, queryIds.get(0), null);
        
        // this should return immediately
        ResponseEntity<QueryImplListResponse> listResponse = listFuture.get();
        
        Assert.assertEquals(200, listResponse.getStatusCodeValue());
        
        QueryImplListResponse result = listResponse.getBody();
        
        Assert.assertEquals(1, result.getNumResults());
        
        List<String> actualQueryIds = result.getQuery().stream().map(Query::getId).map(UUID::toString).collect(Collectors.toList());
        
        Assert.assertEquals(queryIds.get(0), actualQueryIds.get(0));
    }
    
    @Test
    public void testListSuccess_filterOnQueryName() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        String uniqueQueryName = "Unique Query";
        
        // define a bunch of queries as the original user
        List<String> queryIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            MultiValueMap<String,String> params = createParams();
            if (i == 0) {
                params.set(QUERY_NAME, uniqueQueryName);
            }
            
            String queryId = createQuery(authUser, params);
            mockServer.reset();
            
            queryIds.add(queryId);
        }
        
        // list queries
        Future<ResponseEntity<QueryImplListResponse>> listFuture = listQueries(authUser, null, uniqueQueryName);
        
        // this should return immediately
        ResponseEntity<QueryImplListResponse> listResponse = listFuture.get();
        
        Assert.assertEquals(200, listResponse.getStatusCodeValue());
        
        QueryImplListResponse result = listResponse.getBody();
        
        Assert.assertEquals(1, result.getNumResults());
        
        List<String> actualQueryIds = result.getQuery().stream().map(Query::getId).map(UUID::toString).collect(Collectors.toList());
        
        Assert.assertEquals(queryIds.get(0), actualQueryIds.get(0));
    }
    
    @Test
    public void testListSuccess_filterOnMultiple() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        String uniqueQueryName = "Unique Query";
        
        // define a bunch of queries as the original user
        List<String> queryIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            MultiValueMap<String,String> params = createParams();
            if (i == 0) {
                params.set(QUERY_NAME, uniqueQueryName);
            }
            
            String queryId = createQuery(authUser, params);
            mockServer.reset();
            
            queryIds.add(queryId);
        }
        
        // list queries with just the query ID and a bogus name
        Future<ResponseEntity<QueryImplListResponse>> listFuture = listQueries(authUser, queryIds.get(0), "bogus name");
        
        // this should return immediately
        ResponseEntity<QueryImplListResponse> listResponse = listFuture.get();
        
        Assert.assertEquals(200, listResponse.getStatusCodeValue());
        
        QueryImplListResponse result = listResponse.getBody();
        
        Assert.assertEquals(0, result.getNumResults());
        
        // list queries with just the query name and a bogus ID
        listFuture = listQueries(authUser, UUID.randomUUID().toString(), uniqueQueryName);
        
        // this should return immediately
        listResponse = listFuture.get();
        
        Assert.assertEquals(200, listResponse.getStatusCodeValue());
        
        result = listResponse.getBody();
        
        Assert.assertEquals(0, result.getNumResults());
        
        // list queries with just the query name and a bogus ID
        listFuture = listQueries(authUser, queryIds.get(0), uniqueQueryName);
        
        // this should return immediately
        listResponse = listFuture.get();
        
        Assert.assertEquals(200, listResponse.getStatusCodeValue());
        
        result = listResponse.getBody();
        
        Assert.assertEquals(1, result.getNumResults());
        
        List<String> actualQueryIds = result.getQuery().stream().map(Query::getId).map(UUID::toString).collect(Collectors.toList());
        
        Assert.assertEquals(queryIds.get(0), actualQueryIds.get(0));
    }
    
    @Test
    public void testListFailure_ownershipFailure() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        ProxiedUserDetails altAuthUser = createAltUserDetails();
        
        String uniqueQueryName = "Unique Query";
        
        // define a bunch of queries as the original user
        List<String> queryIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            MultiValueMap<String,String> params = createParams();
            if (i == 0) {
                params.set(QUERY_NAME, uniqueQueryName);
            }
            
            String queryId = createQuery(authUser, params);
            mockServer.reset();
            
            queryIds.add(queryId);
        }
        
        // list queries with just the query ID and a bogus name
        Future<ResponseEntity<QueryImplListResponse>> listFuture = listQueries(altAuthUser, queryIds.get(0), "bogus name");
        
        // this should return immediately
        ResponseEntity<QueryImplListResponse> listResponse = listFuture.get();
        
        Assert.assertEquals(200, listResponse.getStatusCodeValue());
        
        QueryImplListResponse result = listResponse.getBody();
        
        Assert.assertEquals(0, result.getNumResults());
        
        // list queries with just the query name and a bogus ID
        listFuture = listQueries(altAuthUser, UUID.randomUUID().toString(), uniqueQueryName);
        
        // this should return immediately
        listResponse = listFuture.get();
        
        Assert.assertEquals(200, listResponse.getStatusCodeValue());
        
        result = listResponse.getBody();
        
        Assert.assertEquals(0, result.getNumResults());
        
        // list queries with the query name and query ID
        listFuture = listQueries(altAuthUser, queryIds.get(0), uniqueQueryName);
        
        // this should return immediately
        listResponse = listFuture.get();
        
        Assert.assertEquals(200, listResponse.getStatusCodeValue());
        
        result = listResponse.getBody();
        
        Assert.assertEquals(0, result.getNumResults());
    }
    
    @Test
    public void testAdminListSuccess() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        ProxiedUserDetails adminUser = createAltUserDetails(Arrays.asList("AuthorizedUser", "Administrator"), null);
        
        String user = ProxiedEntityUtils.getShortName(authUser.getPrimaryUser().getDn().subjectDN());
        
        String uniqueQueryName = "Unique Query";
        
        // define a bunch of queries as the original user
        List<String> queryIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            MultiValueMap<String,String> params = createParams();
            if (i == 0) {
                params.set(QUERY_NAME, uniqueQueryName);
            }
            
            String queryId = createQuery(authUser, params);
            mockServer.reset();
            
            queryIds.add(queryId);
        }
        
        // list queries with just the query ID
        Future<ResponseEntity<QueryImplListResponse>> listFuture = adminListQueries(adminUser, queryIds.get(0), user, null);
        
        // this should return immediately
        ResponseEntity<QueryImplListResponse> listResponse = listFuture.get();
        
        Assert.assertEquals(200, listResponse.getStatusCodeValue());
        
        QueryImplListResponse result = listResponse.getBody();
        
        Assert.assertEquals(1, result.getNumResults());
        
        List<String> actualQueryIds = result.getQuery().stream().map(Query::getId).map(UUID::toString).collect(Collectors.toList());
        
        Assert.assertEquals(queryIds.get(0), actualQueryIds.get(0));
        
        // list queries with just the query name
        listFuture = adminListQueries(adminUser, null, user, uniqueQueryName);
        
        // this should return immediately
        listResponse = listFuture.get();
        
        Assert.assertEquals(200, listResponse.getStatusCodeValue());
        
        result = listResponse.getBody();
        
        Assert.assertEquals(1, result.getNumResults());
        
        actualQueryIds = result.getQuery().stream().map(Query::getId).map(UUID::toString).collect(Collectors.toList());
        
        Assert.assertEquals(queryIds.get(0), actualQueryIds.get(0));
        
        // list queries with the query name and query ID
        listFuture = adminListQueries(adminUser, queryIds.get(0), user, uniqueQueryName);
        
        // this should return immediately
        listResponse = listFuture.get();
        
        Assert.assertEquals(200, listResponse.getStatusCodeValue());
        
        result = listResponse.getBody();
        
        Assert.assertEquals(1, result.getNumResults());
        
        actualQueryIds = result.getQuery().stream().map(Query::getId).map(UUID::toString).collect(Collectors.toList());
        
        Assert.assertEquals(queryIds.get(0), actualQueryIds.get(0));
    }
    
    @Test
    public void testAdminListFailure_notAdminUser() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        ProxiedUserDetails altAuthUser = createAltUserDetails();
        
        String user = ProxiedEntityUtils.getShortName(authUser.getPrimaryUser().getDn().subjectDN());
        
        String uniqueQueryName = "Unique Query";
        
        // define a bunch of queries as the original user
        List<String> queryIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            MultiValueMap<String,String> params = createParams();
            if (i == 0) {
                params.set(QUERY_NAME, uniqueQueryName);
            }
            
            String queryId = createQuery(authUser, params);
            mockServer.reset();
            
            queryIds.add(queryId);
        }
        
        UriComponentsBuilder uriBuilder = uriBuilder("/adminList");
        UriComponents uri = uriBuilder.build();
        
        RequestEntity requestEntity = jwtRestTemplate.createRequestEntity(altAuthUser, null, null, HttpMethod.GET, uri);
        
        // make the next call asynchronously
        Future<ResponseEntity<String>> listFuture = Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, String.class));
        
        ResponseEntity<String> listResponse = listFuture.get();
        
        Assert.assertEquals(403, listResponse.getStatusCodeValue());
    }
    
    @Test
    public void testGetQuerySuccess() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // define a query
        String queryId = createQuery(authUser, createParams());
        mockServer.reset();
        
        // get the query
        Future<ResponseEntity<QueryImplListResponse>> listFuture = getQuery(authUser, queryId);
        
        // this should return immediately
        ResponseEntity<QueryImplListResponse> listResponse = listFuture.get();
        
        Assert.assertEquals(200, listResponse.getStatusCodeValue());
        
        QueryImplListResponse result = listResponse.getBody();
        
        Assert.assertEquals(1, result.getNumResults());
        
        Assert.assertEquals(queryId, result.getQuery().get(0).getId().toString());
    }
    
    @Test
    public void testListQueryLogicSuccess() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        Future<ResponseEntity<QueryLogicResponse>> future = listQueryLogic(authUser);
        
        ResponseEntity<QueryLogicResponse> response = future.get();
        
        Assert.assertEquals(200, response.getStatusCodeValue());
        
        QueryLogicResponse qlResponse = response.getBody();
        
        String[] expectedQueryLogics = new String[] {"AltEventQuery", "ContentQuery", "CountQuery", "DiscoveryQuery", "EdgeEventQuery", "EdgeQuery",
                "ErrorCountQuery", "ErrorDiscoveryQuery", "ErrorEventQuery", "ErrorFieldIndexCountQuery", "EventQuery", "FacetedQuery", "FieldIndexCountQuery",
                "HitHighlights", "IndexStatsQuery", "LuceneUUIDEventQuery", "QueryMetricsQuery", "TermFrequencyQuery"};
        
        Assert.assertEquals(expectedQueryLogics.length, qlResponse.getQueryLogicList().size());
        
        List<String> qlNames = qlResponse.getQueryLogicList().stream().map(QueryLogicDescription::getName).sorted().collect(Collectors.toList());
        
        qlNames.removeAll(Arrays.asList(expectedQueryLogics));
        
        Assert.assertTrue(qlNames.isEmpty());
    }
}
