package datawave.microservice.query.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * This callback is used to retrieve a result payload from the ClaimCheck
 */
public interface ClaimCheckCallback {
    
    Object getPayload() throws InterruptedException, JsonProcessingException;
}
