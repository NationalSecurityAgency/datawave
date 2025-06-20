package datawave.webservice.common.audit.remote;

import java.security.Principal;
import java.util.Map;

import javax.annotation.Resource;
import javax.ejb.EJBContext;
import javax.inject.Inject;

import org.apache.http.Consts;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.annotation.Metric;
import com.codahale.metrics.annotation.Timed;

import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.common.audit.AuditService;
import datawave.webservice.common.remote.RemoteHttpService;

/**
 * This default auditor sends audits to a remote audit microservice.
 */
public class RemoteAuditor extends RemoteHttpService implements AuditService {

    private String auditServiceURI = "/audit/v1/";

    @Inject
    @Metric(name = "dw.remoteDatawaveAuditService.retries", absolute = true)
    private Counter retryCounter;

    @Inject
    @Metric(name = "dw.remoteDatawaveAuditService.failures", absolute = true)
    private Counter failureCounter;

    @Resource
    private EJBContext ctx;

    @Override
    @Timed(name = "dw.remoteAuditService.audit", absolute = true)
    public String audit(Map<String,String> params) {
        Principal p = ctx.getCallerPrincipal();
        DatawavePrincipal dp = null;
        if (p instanceof DatawavePrincipal)
            dp = (DatawavePrincipal) p;

        final String bearerHeader = "Bearer " + jwtTokenHandler.createTokenFromUsers(dp.getName(), dp.getProxiedUsers());
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
}
