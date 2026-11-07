package datawave.resteasy.interceptor;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import jakarta.annotation.Priority;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.Provider;
import jakarta.ws.rs.ext.WriterInterceptorContext;

import org.apache.log4j.Logger;
import org.jboss.resteasy.core.interception.jaxrs.PreMatchContainerRequestContext;

@Provider
@Priority(Priorities.USER)
public class LoggingInterceptor extends BaseMethodStatsInterceptor {
    private Logger log = Logger.getLogger(this.getClass());

    @Override
    public void aroundWriteTo(WriterInterceptorContext context) throws IOException, WebApplicationException {
        if (!log.isTraceEnabled())
            return;

        ResponseMethodStats stats = doWrite(context);
        StringBuilder message = new StringBuilder();
        message.append(" Post Process: StatusCode: ").append(stats.getStatusCode());
        message.append(" Response Headers {");
        for (Entry<String,List<Object>> header : stats.getResponseHeaders().entrySet()) {
            message.append(" ").append(header.getKey()).append(" -> ");
            String sep = "";
            for (Object o : header.getValue()) {
                message.append(sep).append(o);
                sep = ",";
            }
        }
        message.append("} Serialization time: ").append(stats.getSerializationTime()).append("ms");
        message.append(" Bytes written: ").append(stats.getBytesWritten());
        message.append(" Login Time: ").append(stats.getLoginTime()).append("ms");
        message.append(" Call Time: ").append(stats.getCallTime()).append("ms");

        log.trace(message);
    }

    @Override
    public void filter(ContainerRequestContext request) throws IOException {
        if (!log.isTraceEnabled())
            return;

        RequestMethodStats stats = doPreProcess((PreMatchContainerRequestContext) request);

        StringBuilder message = new StringBuilder();
        message.append(" URI: ").append(stats.getUri());
        message.append(" Method: ").append(stats.getMethod());
        message.append(" Request Headers {");
        for (Entry<String,List<String>> header : stats.getRequestHeaders().entrySet()) {
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
            MultivaluedMap<String,String> formParams = stats.getFormParameters();
            if (formParams == null || formParams.isEmpty()) {
                message.append(" None ");
            } else {
                for (Entry<String,List<String>> header : formParams.entrySet()) {
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
