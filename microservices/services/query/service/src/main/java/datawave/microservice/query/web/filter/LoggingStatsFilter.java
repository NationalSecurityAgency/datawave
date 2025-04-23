package datawave.microservice.query.web.filter;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;

@Component
public class LoggingStatsFilter extends BaseMethodStatsFilter {
    private final Logger log = Logger.getLogger(this.getClass());
    
    @Override
    public void preProcess(RequestMethodStats requestStats) {
        if (!log.isTraceEnabled() && requestStats != null) {
            StringBuilder message = new StringBuilder();
            message.append(" URI: ").append(requestStats.getUri());
            message.append(" Method: ").append(requestStats.getMethod());
            message.append(" Request Headers {");
            for (Map.Entry<String,List<String>> header : requestStats.getRequestHeaders().entrySet()) {
                message.append(" ").append(header.getKey()).append(" -> ");
                String sep = "";
                for (Object o : header.getValue()) {
                    message.append(sep).append(o);
                    sep = ",";
                }
            }
            message.append("}");
            message.append(" Form Parameters {");
            try {
                MultiValueMap<String,String> formParams = requestStats.getFormParameters();
                if (formParams == null || formParams.isEmpty()) {
                    message.append(" None ");
                } else {
                    for (Map.Entry<String,List<String>> header : formParams.entrySet()) {
                        message.append(" ").append(header.getKey()).append(" -> ");
                        String sep = "";
                        for (Object o : header.getValue()) {
                            message.append(sep).append(o);
                            sep = ",";
                        }
                    }
                }
            } catch (NullPointerException npe) {
                log.warn("Unable to log request due to NPE");
            } catch (Exception e) {
                if (null != e.getMessage())
                    log.warn("Unable to log request due to error: " + e.getMessage());
                else
                    log.warn("Unable to log request due to error", e);
            }
            message.append("}");
            log.trace(message.toString());
        }
    }
    
    @Override
    public void postProcess(ResponseMethodStats responseStats) {
        if (!log.isTraceEnabled() && responseStats != null) {
            StringBuilder message = new StringBuilder();
            message.append(" Post Process: StatusCode: ").append(responseStats.getStatusCode());
            message.append(" Response Headers {");
            for (Map.Entry<String,List<Object>> header : responseStats.getResponseHeaders().entrySet()) {
                message.append(" ").append(header.getKey()).append(" -> ");
                String sep = "";
                for (Object o : header.getValue()) {
                    message.append(sep).append(o);
                    sep = ",";
                }
            }
            message.append("} Serialization time: ").append(responseStats.getSerializationTime()).append("ms");
            message.append(" Bytes written: ").append(responseStats.getBytesWritten());
            message.append(" Login Time: ").append(responseStats.getLoginTime()).append("ms");
            message.append(" Call Time: ").append(responseStats.getCallTime()).append("ms");
            
            log.trace(message);
        }
    }
}
