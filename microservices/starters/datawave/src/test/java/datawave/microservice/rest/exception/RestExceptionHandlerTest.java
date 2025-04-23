package datawave.microservice.rest.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;

import datawave.microservice.config.web.Constants;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.VoidResponse;
import reactor.core.publisher.Mono;

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
        
        ResponseEntity<VoidResponse> responseEntity = webClient.get().uri("/testSingleQueryException").retrieve()
                        .onStatus(HttpStatus::isError, response -> Mono.empty()).toEntity(VoidResponse.class).block();
        assertNotNull(responseEntity);
        assertEquals(400, responseEntity.getStatusCodeValue());
        
        HttpHeaders headers = responseEntity.getHeaders();
        assertTrue(headers.containsKey(Constants.ERROR_CODE_HEADER), "ErrorCode header was missing from failed result.");
        assertEquals(expectedErrorCode, headers.getFirst(Constants.ERROR_CODE_HEADER));
        
        VoidResponse vr = responseEntity.getBody();
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
        
        ResponseEntity<VoidResponse> responseEntity = webClient.get().uri("/testNestedQueryException").retrieve()
                        .onStatus(HttpStatus::isError, response -> Mono.empty()).toEntity(VoidResponse.class).block();
        assertNotNull(responseEntity);
        assertEquals(500, responseEntity.getStatusCodeValue());
        
        HttpHeaders headers = responseEntity.getHeaders();
        assertTrue(headers.containsKey(Constants.ERROR_CODE_HEADER), "ErrorCode header was missing from failed result.");
        assertEquals(expectedErrorCode, headers.getFirst(Constants.ERROR_CODE_HEADER));
        
        VoidResponse vr = responseEntity.getBody();
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
        
        ResponseEntity<VoidResponse> responseEntity = webClient.get().uri("/testNonQueryException").retrieve()
                        .onStatus(HttpStatus::isError, response -> Mono.empty()).toEntity(VoidResponse.class).block();
        assertNotNull(responseEntity);
        assertEquals(500, responseEntity.getStatusCodeValue());
        
        HttpHeaders headers = responseEntity.getHeaders();
        assertFalse(headers.containsKey(Constants.ERROR_CODE_HEADER), "ErrorCode header was set from non-query failed result.");
        
        VoidResponse vr = responseEntity.getBody();
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
