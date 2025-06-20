package datawave.webservice.operations.remote;

import java.util.function.Supplier;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;

import com.codahale.metrics.Counter;
import com.codahale.metrics.annotation.Metric;
import com.fasterxml.jackson.databind.ObjectReader;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.system.CallerPrincipal;
import datawave.webservice.common.remote.RemoteHttpService;

public abstract class RemoteAccumuloService extends RemoteHttpService {

    protected static final String AUTH_HEADER_NAME = "Authorization";

    private String serviceURI = "/accumulo/v1/";

    @Inject
    @Metric(name = "dw.remoteAccumuloService.retries", absolute = true)
    private Counter retryCounter;

    @Inject
    @Metric(name = "dw.remoteAccumuloService.failures", absolute = true)
    private Counter failureCounter;

    @Inject
    @CallerPrincipal
    protected DatawavePrincipal callerPrincipal;

    @Override
    @PostConstruct
    public void init() {
        super.init();
    }

    protected String getBearer() {
        return "Bearer " + jwtTokenHandler.createTokenFromUsers(callerPrincipal.getName(), callerPrincipal.getProxiedUsers());
    }

    protected <T> T execGet(String suffix, ObjectReader reader) {
        // @formatter:off
        return executeGetMethodWithRuntimeException(
                suffix,
                uriBuilder -> {},
                httpGet -> {
                    httpGet.setHeader(AUTH_HEADER_NAME, getBearer());
                    httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                },
                entity -> reader.readValue(entity.getContent()),
                () -> StringUtils.isEmpty(suffix) ? serviceURI() : suffix
        );
        // @formatter:on
    }

    protected <T> T execPost(String suffix, ObjectReader reader) {
        return execPost(suffix, reader, null);
    }

    protected <T> T execPost(String suffix, ObjectReader reader, HttpEntity postBody) {
        return execPost(suffix, reader, postBody, () -> StringUtils.isEmpty(suffix) ? serviceURI() : suffix);
    }

    protected <T> T execPost(String suffix, ObjectReader reader, HttpEntity postBody, Supplier<String> errorSupplier) {
        // @formatter:off
        return executePostMethodWithRuntimeException(
                suffix,
                uriBuilder -> {},
                httpPost -> {
                    httpPost.setEntity(postBody);
                    httpPost.setHeader(AUTH_HEADER_NAME, getBearer());
                    httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                },
                entity -> reader.readValue(entity.getContent()),
                errorSupplier
        );
        // @formatter:on
    }

    @Override
    protected Counter retryCounter() {
        return retryCounter;
    }

    @Override
    protected String serviceURI() {
        return serviceURI;
    }

    @Override
    protected Counter failureCounter() {
        return failureCounter;
    }
}
