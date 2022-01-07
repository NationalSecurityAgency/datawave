package datawave.microservice.query.web.filter;

import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;

public class CountingResponseBodyEmitter extends ResponseBodyEmitter {
    private final BaseMethodStatsFilter.CountingHttpServletResponseWrapper countingResponse;
    
    CountingResponseBodyEmitter(BaseMethodStatsFilter.CountingHttpServletResponseWrapper countingResponse) {
        this.countingResponse = countingResponse;
    }
    
    public long getBytesWritten() {
        return (countingResponse != null) ? countingResponse.getBytesWritten() : 0L;
    }
}
