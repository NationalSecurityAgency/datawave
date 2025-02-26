package datawave.microservice.query.stream.listener;

import java.io.IOException;

import org.springframework.http.MediaType;

import datawave.microservice.query.web.filter.CountingResponseBodyEmitter;
import datawave.webservice.result.BaseQueryResponse;

public class CountingResponseBodyEmitterListener implements StreamingResponseListener {
    private final CountingResponseBodyEmitter countingEmitter;
    private final MediaType mediaType;
    
    public CountingResponseBodyEmitterListener(CountingResponseBodyEmitter countingEmitter, MediaType mediaType) {
        this.countingEmitter = countingEmitter;
        this.mediaType = mediaType;
    }
    
    @Override
    public void onResponse(BaseQueryResponse response) throws IOException {
        countingEmitter.send(response, mediaType);
    }
    
    @Override
    public void close() {
        countingEmitter.complete();
    }
    
    @Override
    public void closeWithError(Throwable t) {
        countingEmitter.completeWithError(t);
    }
    
    public CountingResponseBodyEmitter getCountingEmitter() {
        return countingEmitter;
    }
    
    public MediaType getMediaType() {
        return mediaType;
    }
    
    public long getBytesWritten() {
        return (countingEmitter != null) ? countingEmitter.getBytesWritten() : 0L;
    }
}
