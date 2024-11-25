package datawave.microservice.query.stream.listener;

import java.io.IOException;

import datawave.webservice.result.BaseQueryResponse;

public interface StreamingResponseListener {
    
    void onResponse(BaseQueryResponse response) throws IOException;
    
    default void close() {
        // do nothing
    }
    
    default void closeWithError(Throwable t) {
        // do nothing
    }
}
