package datawave.microservice.query.stream;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.authorization.service.RemoteAuthorizationServiceUserDetailsService;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.AbstractQueryServiceTest;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.result.DefaultEventQueryResponse;

import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"QueryStarterDefaults", "QueryStarterOverrides", "QueryServiceTest", RemoteAuthorizationServiceUserDetailsService.ACTIVATION_PROFILE})
@Execution(SAME_THREAD)
public class StreamingServiceTest extends AbstractQueryServiceTest {
    
    @Test
    public void testExecuteSuccess() throws Throwable {
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
                (int)TEST_MAX_RESULTS_OVERRIDE,
                fieldValues,
                "ALL");
        // @formatter:on
        
        // make the execute call asynchronously
        Future<ResponseEntity<String>> future = execute(authUser, queryId);
        
        // the response should come back right away
        ResponseEntity<String> response = future.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        // verify some headers
        Assertions.assertEquals(MediaType.APPLICATION_XML, response.getHeaders().getContentType());
        
        int pageNumber = 1;
        
        List<DefaultEventQueryResponse> queryResponses = parseXMLBaseQueryResponses(response.getBody());
        for (DefaultEventQueryResponse queryResponse : queryResponses) {
            // verify the query response
            // @formatter:off
            assertQueryResponse(
                    queryId,
                    "EventQuery",
                    pageNumber++,
                    false,
                    queryResponse.getOperationTimeMS(),
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
        }
        
        // verify that the next event was published
        Assertions.assertEquals(6, queryRequestEvents.size());
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
                QueryRequest.Method.NEXT,
                queryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.NEXT,
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
    public void testCreateAndExecuteSuccess() throws Throwable {
        DatawaveUserDetails authUser = createUserDetails();
        
        final String query = TEST_QUERY_STRING + " CREATE_AND_NEXT:TEST";
        
        MultiValueMap<String,String> params = createParams();
        params.set(DefaultQueryParameters.QUERY_STRING, query);
        
        // make the execute call asynchronously
        Future<ResponseEntity<String>> future = createAndExecute(authUser, params);
        
        long startTime = System.currentTimeMillis();
        QueryStatus queryStatus = null;
        while (queryStatus == null && (System.currentTimeMillis() - startTime) < TEST_WAIT_TIME_MILLIS) {
            queryStatus = queryStorageCache.getQueryStatus().stream().filter(x -> x.getQuery().getQuery().equals(query)).findAny().orElse(null);
            if (queryStatus == null) {
                Thread.sleep(500);
            }
        }
        
        String queryId = queryStatus.getQueryKey().getQueryId();
        
        // pump enough results into the queue to trigger a complete page
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
                (int)TEST_MAX_RESULTS_OVERRIDE,
                fieldValues,
                "ALL");
        // @formatter:on
        
        // the response should come back right away
        ResponseEntity<String> response = future.get();
        
        Assertions.assertEquals(200, response.getStatusCodeValue());
        
        // verify some headers
        Assertions.assertEquals(MediaType.APPLICATION_XML, response.getHeaders().getContentType());
        
        int pageNumber = 1;
        
        List<DefaultEventQueryResponse> queryResponses = parseXMLBaseQueryResponses(response.getBody());
        for (DefaultEventQueryResponse queryResponse : queryResponses) {
            // verify the query response
            // @formatter:off
            assertQueryResponse(
                    queryId,
                    "EventQuery",
                    pageNumber++,
                    false,
                    queryResponse.getOperationTimeMS(),
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
        }
        
        // verify that the next event was published
        Assertions.assertEquals(6, queryRequestEvents.size());
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
                QueryRequest.Method.NEXT,
                queryId,
                queryRequestEvents.removeLast());
        assertQueryRequestEvent(
                "executor-unassigned:**",
                QueryRequest.Method.NEXT,
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
    
    protected Future<ResponseEntity<String>> createAndExecute(DatawaveUserDetails authUser, MultiValueMap<String,String> map) {
        UriComponents uri = createUri("EventQuery/createAndExecute");
        
        // not testing audit with this method
        auditIgnoreSetup();
        
        MultiValueMap<String,String> headers = new LinkedMultiValueMap<>();
        headers.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_XML_VALUE);
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, map, headers, HttpMethod.POST, uri);
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, String.class));
    }
    
    protected Future<ResponseEntity<String>> execute(DatawaveUserDetails authUser, String queryId) {
        UriComponents uri = createUri(queryId + "/execute");
        
        MultiValueMap<String,String> headers = new LinkedMultiValueMap<>();
        headers.set(HttpHeaders.ACCEPT, MediaType.APPLICATION_XML_VALUE);
        
        RequestEntity<MultiValueMap<String,String>> requestEntity = jwtRestTemplate.createRequestEntity(authUser, null, headers, HttpMethod.GET, uri);
        return Executors.newSingleThreadExecutor().submit(() -> jwtRestTemplate.exchange(requestEntity, String.class));
    }
    
    private ObjectMapper createJSONObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JaxbAnnotationModule());
        mapper.configure(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME, true);
        return mapper;
    }
    
    protected List<DefaultEventQueryResponse> parseJSONBaseQueryResponses(String responseBody) throws JsonProcessingException {
        String delimiter = "}{";
        ObjectMapper mapper = createJSONObjectMapper();
        List<DefaultEventQueryResponse> baseResponses = new ArrayList<>();
        int start = 0;
        int end = responseBody.indexOf(delimiter) + 1;
        while (end > start) {
            String stringResponse = responseBody.substring(start, end);
            baseResponses.add(mapper.readValue(stringResponse, DefaultEventQueryResponse.class));
            start = end;
            end = responseBody.indexOf(delimiter, start) + 1;
            if (end == 0) {
                end = responseBody.length();
            }
        }
        return baseResponses;
    }
    
    protected List<DefaultEventQueryResponse> parseXMLBaseQueryResponses(String responseBody) throws JAXBException {
        String delimiter = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>";
        List<DefaultEventQueryResponse> baseResponses = new ArrayList<>();
        int start = responseBody.indexOf(delimiter);
        int end = responseBody.indexOf(delimiter, start + delimiter.length());
        while (end > start) {
            String stringResponse = responseBody.substring(start, end);
            
            JAXBContext jaxbContext = JAXBContext.newInstance(DefaultEventQueryResponse.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            baseResponses.add((DefaultEventQueryResponse) unmarshaller.unmarshal(new StringReader(stringResponse)));
            
            start = end;
            end = responseBody.indexOf(delimiter, start + delimiter.length());
            if (end == -1) {
                end = responseBody.length();
            }
        }
        return baseResponses;
    }
}
