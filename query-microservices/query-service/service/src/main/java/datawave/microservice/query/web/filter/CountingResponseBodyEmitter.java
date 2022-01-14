package datawave.microservice.query.web.filter;

import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;

public class CountingResponseBodyEmitter extends ResponseBodyEmitter {
    private final BaseMethodStatsFilter.CountingHttpServletResponseWrapper countingResponse;
    
    public CountingResponseBodyEmitter(Long timeout, BaseMethodStatsFilter.CountingHttpServletResponseWrapper countingResponse) {
        super(timeout);
        this.countingResponse = countingResponse;
    }
    
    public long getBytesWritten() {
        return (countingResponse != null) ? countingResponse.getBytesWritten() : 0L;
    }
}
