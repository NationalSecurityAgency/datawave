package datawave.microservice.rest.exception;

import datawave.microservice.config.web.Constants;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"exceptionMapperTest", "permitAllWebTest"})
public class RestExceptionHandlerTest {
    
    @LocalServerPort
    private int webServicePort;
    
    @Autowired
    private WebClient.Builder webClientBuilder;
    
    @Autowired
    private TestOperations testOperations;
    
    @Test
    public void testSingleQueryException() {
        
        String expectedErrorCode = "400-1234";
        testOperations.setErrorCode(expectedErrorCode);
        
        WebClient webClient = webClientBuilder.baseUrl("https://localhost:" + webServicePort + "/starter-test/v1").build();
        
        ClientResponse clientResponse = webClient.get().uri("/testSingleQueryException").exchange().block();
        assertNotNull(clientResponse);
        assertEquals(400, clientResponse.rawStatusCode());
        
        HttpHeaders headers = clientResponse.headers().asHttpHeaders();
        assertTrue("ErrorCode header was missing from failed result.", headers.containsKey(Constants.ERROR_CODE_HEADER));
        assertEquals(expectedErrorCode, headers.getFirst(Constants.ERROR_CODE_HEADER));
        
        VoidResponse vr = clientResponse.bodyToMono(VoidResponse.class).block();
        assertNotNull(vr);
        assertNotNull(vr.getExceptions());
        assertEquals(1, vr.getExceptions().size());
        assertEquals("test exception", vr.getExceptions().get(0).getMessage());
        assertEquals(expectedErrorCode, vr.getExceptions().get(0).getCode());
        assertEquals("Exception with no cause caught", vr.getExceptions().get(0).getCause());
    }
    
    @Test
    public void testNestedQueryException() {
        
        String expectedErrorCode = "500-9999";
        testOperations.setErrorCode(expectedErrorCode);
        
        WebClient webClient = webClientBuilder.baseUrl("https://localhost:" + webServicePort + "/starter-test/v1").build();
        
        ClientResponse clientResponse = webClient.get().uri("/testNestedQueryException").exchange().block();
        assertNotNull(clientResponse);
        assertEquals(500, clientResponse.rawStatusCode());
        
        HttpHeaders headers = clientResponse.headers().asHttpHeaders();
        assertTrue("ErrorCode header was missing from failed result.", headers.containsKey(Constants.ERROR_CODE_HEADER));
        assertEquals(expectedErrorCode, headers.getFirst(Constants.ERROR_CODE_HEADER));
        
        VoidResponse vr = clientResponse.bodyToMono(VoidResponse.class).block();
        assertNotNull(vr);
        assertNotNull(vr.getExceptions());
        assertEquals(1, vr.getExceptions().size());
        assertEquals("nested exception", vr.getExceptions().get(0).getMessage());
        assertEquals("400-1", vr.getExceptions().get(0).getCode());
        assertEquals(QueryException.class.getName() + ": nested exception", vr.getExceptions().get(0).getCause());
    }
    
    @Test
    public void testNonQueryException() {
        
        String expectedErrorCode = "400-9999";
        testOperations.setErrorCode(expectedErrorCode);
        
        WebClient webClient = webClientBuilder.baseUrl("https://localhost:" + webServicePort + "/starter-test/v1").build();
        
        ClientResponse clientResponse = webClient.get().uri("/testNonQueryException").exchange().block();
        assertNotNull(clientResponse);
        assertEquals(500, clientResponse.rawStatusCode());
        
        HttpHeaders headers = clientResponse.headers().asHttpHeaders();
        assertFalse("ErrorCode header was set from non-query failed result.", headers.containsKey(Constants.ERROR_CODE_HEADER));
        
        VoidResponse vr = clientResponse.bodyToMono(VoidResponse.class).block();
        assertNotNull(vr);
        assertNotNull(vr.getExceptions());
        assertEquals(1, vr.getExceptions().size());
        assertEquals("This is a non-query exception.", vr.getExceptions().get(0).getMessage());
        assertNull(vr.getExceptions().get(0).getCode());
        assertEquals("Exception with no cause caught", vr.getExceptions().get(0).getCause());
    }
    
    @SpringBootApplication(scanBasePackages = "datawave.microservice")
    public static class TestConfiguration {}
}

@RestController
@RequestMapping(path = "/v1", produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE})
class TestOperations {
    private String errorCode = "";
    
    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }
    
    @RequestMapping("/testNonQueryException")
    public String testNonQueryException() {
        throw new RuntimeException("This is a non-query exception.");
    }
    
    @RequestMapping("/testSingleQueryException")
    public String testSingleQueryException() throws QueryException {
        throw new QueryException("test exception", errorCode);
    }
    
    @RequestMapping("/testNestedQueryException")
    public String testNestedQueryException() throws QueryException {
        QueryException qe = new QueryException("nested exception", new Exception("cause exception"), errorCode);
        throw new QueryException("test exception", qe, "400-1");
    }
}
