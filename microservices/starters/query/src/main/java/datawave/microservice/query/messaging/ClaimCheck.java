package datawave.microservice.query.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;

public interface ClaimCheck {
    <T> void check(String queryId, T data) throws InterruptedException, JsonProcessingException;
    
    <T> T claim(String queryId) throws InterruptedException, JsonProcessingException;
    
    void empty(String queryId);
    
    void delete(String queryId);
}
