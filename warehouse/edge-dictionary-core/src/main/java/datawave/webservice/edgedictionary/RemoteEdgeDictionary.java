package datawave.webservice.edgedictionary;

import java.net.URI;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.http.client.utils.URIBuilder;
import org.xbill.DNS.TextParseException;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectReader;

import datawave.microservice.query.Query;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.system.CallerPrincipal;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.dictionary.edge.EdgeDictionaryBase;
import datawave.webservice.dictionary.edge.MetadataBase;

/**
 * Retrieves an {@link EdgeDictionaryBase} from the remote edge dictionary service.
 */
public class RemoteEdgeDictionary extends RemoteHttpService {
    private ObjectReader edgeDictReader;

    private boolean useConfiguredURIForRedirect = false;
    private String dictServiceURI = "/dictionary/edge/v1/";

    private MetricRegistry metricRegistry;
    private JavaType edgeDictionaryType;
    private Counter retryCounter;
    private Counter failureCounter;

    @Inject
    @CallerPrincipal
    protected DatawavePrincipal callerPrincipal;

    @Override
    @PostConstruct
    public void init() {
        // retryCounter is used by super.init() when setting the DatawaveRetryHandler
        retryCounter = metricRegistry.counter("dw.remoteEdgeDictionary.retries");
        failureCounter = metricRegistry.counter("dw.remoteEdgeDictionary.failures");
        super.init();
        edgeDictReader = objectMapper.readerFor(edgeDictionaryType);
    }

    public EdgeDictionaryBase<?,? extends MetadataBase<?>> getEdgeDictionary(Query settings, String metadataTableName) {
        final String bearerHeader = "Bearer " + jwtTokenHandler.createTokenFromUsers(callerPrincipal.getName(), callerPrincipal.getProxiedUsers());
        // @formatter:off
        return executeGetMethodWithRuntimeException("",
                uriBuilder -> {
                    uriBuilder.addParameter("metadataTableName", metadataTableName);
                    uriBuilder.addParameter("auths", settings.getQueryAuthorizations());
                },
                httpGet -> httpGet.setHeader("Authorization", bearerHeader),
                entity -> edgeDictReader.readValue(entity.getContent()),
                () -> "getEdgeDictionary [" + metadataTableName + ", " + settings.getQueryAuthorizations() + "]");
        // @formatter:on
    }

    public URIBuilder buildRedirectURI(String suffix, URI baseURI) throws TextParseException {
        return buildRedirectURI(suffix, baseURI, useConfiguredURIForRedirect);
    }

    public void setUseConfiguredURIForRedirect(boolean useConfiguredURIForRedirect) {
        this.useConfiguredURIForRedirect = useConfiguredURIForRedirect;
    }

    @Override
    protected String serviceURI() {
        return dictServiceURI;
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

    public void setEdgeDictionaryType(JavaType edgeDictionaryType) {
        this.edgeDictionaryType = edgeDictionaryType;
    }
}
