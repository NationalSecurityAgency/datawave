package datawave.microservice.query.stream.listener;

import datawave.microservice.query.web.filter.CountingResponseBodyEmitter;
import datawave.webservice.result.BaseQueryResponse;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.util.List;

public class CountingResponseBodyEmitterListener implements StreamingResponseListener {
    private final CountingResponseBodyEmitter countingEmitter;
    private final MediaType mediaType;
    
    public CountingResponseBodyEmitterListener(CountingResponseBodyEmitter countingEmitter, List<MediaType> mediaTypes) {
        this.countingEmitter = countingEmitter;
        this.mediaType = determineMediaType(mediaTypes);
    }
    
    @Override
    public void onResponse(BaseQueryResponse response) throws IOException {
        countingEmitter.send(response, mediaType);
    }
    
    @Override
    public void cleanup() {
        countingEmitter.complete();
    }
    
    public long getBytesWritten() {
        return (countingEmitter != null) ? countingEmitter.getBytesWritten() : 0L;
    }
    
    private MediaType determineMediaType(List<MediaType> acceptedMediaTypes) {
        MediaType mediaType = null;
        if (acceptedMediaTypes != null && !acceptedMediaTypes.isEmpty()) {
            MediaType.sortBySpecificityAndQuality(acceptedMediaTypes);
            mediaType = acceptedMediaTypes.get(0);
        }
        return mediaType;
    }
}
