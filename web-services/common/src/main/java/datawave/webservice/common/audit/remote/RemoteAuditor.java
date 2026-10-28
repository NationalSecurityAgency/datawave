package datawave.webservice.common.audit.remote;

import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.http.Consts;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.system.CallerPrincipal;
import datawave.webservice.common.audit.AuditService;
import datawave.webservice.common.remote.RemoteHttpService;

/**
 * This default auditor sends audits to a remote audit microservice.
 */
public class RemoteAuditor extends RemoteHttpService implements AuditService {

    private String auditServiceURI = "/audit/v1/";

    private MetricRegistry metricRegistry;
    private Counter retryCounter;
    private Counter failureCounter;

    @Inject
    @CallerPrincipal
    protected DatawavePrincipal callerPrincipal;

    @Override
    @PostConstruct
    public void init() {
        // retryCounter is used by super.init() when setting the DatawaveRetryHandler
        retryCounter = metricRegistry.counter("dw.remoteAuditService.retries");
        failureCounter = metricRegistry.counter("dw.remoteAuditService.failures");
        super.init();
    }

    @Override
    @Timed(name = "dw.remoteAuditService.audit", absolute = true)
    public String audit(Map<String,String> params) {
        final String bearerHeader = "Bearer " + jwtTokenHandler.createTokenFromUsers(callerPrincipal.getName(), callerPrincipal.getProxiedUsers());
        UrlEncodedFormEntity postBody = new UrlEncodedFormEntity(
                        params.entrySet().stream().map(e -> (NameValuePair) new BasicNameValuePair(e.getKey(), e.getValue()))::iterator, Consts.UTF_8);
        // @formatter:off
        return executePostMethodWithRuntimeException(
                "audit",
                uriBuilder -> {},
                httpPost -> {
                    httpPost.setEntity(postBody);
                    httpPost.setHeader("Authorization", bearerHeader);
                },
                EntityUtils::toString,
                () -> "audit [" + params + "]");
        // @formatter:on
    }

    @Override
    protected String serviceURI() {
        return auditServiceURI;
    }

    @Override
    protected Counter retryCounter() {
        return retryCounter;
    }

    @Override
    protected Counter failureCounter() {
        return failureCounter;
    }

    public void setMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }
}
