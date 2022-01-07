package datawave.microservice.query.stream.listener;

import datawave.webservice.result.BaseQueryResponse;

import java.io.IOException;

public interface StreamingResponseListener {
    
    void onResponse(BaseQueryResponse response) throws IOException;
    
    default void cleanup() {
        // do nothing
    }
}
