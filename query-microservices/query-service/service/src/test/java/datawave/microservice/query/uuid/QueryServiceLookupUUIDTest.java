package datawave.microservice.query.uuid;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.query.AbstractQueryServiceTest;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.webservice.result.BaseQueryResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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

import static datawave.microservice.query.QueryParameters.QUERY_MAX_CONCURRENT_TASKS;
import static datawave.microservice.query.uuid.LookupUUIDService.LOOKUP_UUID_PAIRS;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryStarterDefaults", "QueryStarterOverrides", "QueryServiceTest", RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE})
public class QueryServiceLookupUUIDTest extends AbstractQueryServiceTest {
    @Before
    public void setup() {
        super.setup();
    }
    
    @After
    public void teardown() throws Exception {
        super.teardown();
    }
    
    @Ignore
    @Test
    public void testLookupUUIDSuccess() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        // create a valid query
        long currentTimeMillis = System.currentTimeMillis();
        BaseQueryResponse response = lookupUUID(authUser, createUUIDParams(), "PAGE_TITLE", "anarchy");
        
    }
    
    @Ignore
    @Test
    public void testBatchLookupUUIDSuccess() throws Exception {
        ProxiedUserDetails authUser = createUserDetails();
        
        MultiValueMap<String,String> params = createUUIDParams();
        params.add(LOOKUP_UUID_PAIRS, "PAGE_TITLE:anarchy");
        params.add(LOOKUP_UUID_PAIRS, "PAGE_TITLE:accessiblecomputing");
        
        // create a valid query
        long currentTimeMillis = System.currentTimeMillis();
        BaseQueryResponse response = batchLookupUUID(authUser, params);
        
    }
    
    protected MultiValueMap<String,String> createUUIDParams() {
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.set(DefaultQueryParameters.QUERY_NAME, TEST_QUERY_NAME);
        map.set(DefaultQueryParameters.QUERY_AUTHORIZATIONS, TEST_QUERY_AUTHORIZATIONS);
        map.set(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, TEST_VISIBILITY_MARKING);
        map.set(QUERY_MAX_CONCURRENT_TASKS, Integer.toString(1));
        return map;
    }
    
    protected BaseQueryResponse batchLookupUUID(ProxiedUserDetails authUser, MultiValueMap<String,String> map) {
        UriComponents uri = createUri("lookupUUID");
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        ResponseEntity<BaseQueryResponse> resp = jwtRestTemplate.exchange(requestEntity, BaseQueryResponse.class);
        
        return resp.getBody();
    }
    
    protected BaseQueryResponse lookupUUID(ProxiedUserDetails authUser, MultiValueMap<String,String> map, String uuidType, String uuid) {
        UriComponents uri = createUri("lookupUUID/" + uuidType + "/" + uuid);
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.GET, uri);
        ResponseEntity<BaseQueryResponse> resp = jwtRestTemplate.exchange(requestEntity, BaseQueryResponse.class);
        
        return resp.getBody();
    }
}
