package datawave.microservice.query;

import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;

import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.webservice.result.GenericResponse;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryStarterDefaults", "QueryStarterOverrides", "QueryServiceTest", RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE})
@ContextConfiguration(classes = {QueryService.class})
public class QueryServicePlanTest extends AbstractQueryServiceTest {
    
    @Autowired
    public ApplicationEventPublisher eventPublisher;
    
    @Test
    public void testPlanSuccess() throws ParseException, IOException, ExecutionException, InterruptedException {
        DatawaveUserDetails authUser = createUserDetails();
        UriComponents uri = createUri("EventQuery/plan");
        MultiValueMap<String,String> map = createParams();
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, null, HttpMethod.POST, uri);
        
        // setup a mock audit service
        auditSentSetup();
        
        // make the plan call asynchronously
        Future<ResponseEntity<GenericResponse>> futureResp = Executors.newSingleThreadExecutor()
                        .submit(() -> jwtRestTemplate.exchange(requestEntity, GenericResponse.class));
        
        long startTime = System.currentTimeMillis();
        while (queryRequestEvents.size() == 0 && (System.currentTimeMillis() - startTime) < TEST_WAIT_TIME_MILLIS) {
            Thread.sleep(500);
        }
        
        // verify that the plan event was published
        Assertions.assertEquals(1, queryRequestEvents.size());
        RemoteQueryRequestEvent requestEvent = queryRequestEvents.removeLast();
        
        Assertions.assertEquals("executor-unassigned:**", requestEvent.getDestinationService());
        Assertions.assertEquals(QueryRequest.Method.PLAN, requestEvent.getRequest().getMethod());
        
        String queryId = requestEvent.getRequest().getQueryId();
        String plan = "some plan";
        
        // save the plan to the query status object
        QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
        queryStatus.setPlan(plan);
        queryStorageCache.updateQueryStatus(queryStatus);
        
        // send the plan response
        eventPublisher.publishEvent(new RemoteQueryRequestEvent(this, "executor-unassigned:**", "query:**", QueryRequest.plan(queryId)));
        
        ResponseEntity<GenericResponse> resp = futureResp.get();
        
        // @formatter:off
        GenericResponse<String> genericResponse = assertGenericResponse(
                true,
                HttpStatus.Series.SUCCESSFUL,
                resp);
        // @formatter:on
        
        String receivedPlan = genericResponse.getResult();
        Assertions.assertEquals(receivedPlan, plan);
        
        // @formatter:off
        assertQueryRequestEvent(
                "query:**",
                QueryRequest.Method.PLAN,
                queryId,
                queryRequestEvents.removeLast());
        // @formatter:on
        
        // verify that the query status was deleted
        queryStatus = queryStorageCache.getQueryStatus(queryId);
        Assertions.assertNull(queryStatus);
    }
}
