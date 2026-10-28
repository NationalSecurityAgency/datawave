package datawave.webservice.datadictionary;

import java.net.URI;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.http.client.utils.URIBuilder;
import org.xbill.DNS.TextParseException;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectReader;

import datawave.configuration.RefreshableScope;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.system.CallerPrincipal;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.dictionary.data.DataDictionaryBase;
import datawave.webservice.metadata.MetadataFieldBase;

/**
 * Retrieves an {@link DataDictionaryBase} from the remote edge dictionary service.
 */
@RefreshableScope
public class RemoteDataDictionary extends RemoteHttpService {
    private ObjectReader dataDictReader;

    private boolean useConfiguredURIForRedirect = false;
    private String dictServiceURI = "/dictionary/data/v1/";

    private MetricRegistry metricRegistry;
    private JavaType dataDictionaryType;
    private Counter retryCounter;
    private Counter failureCounter;

    @Inject
    @CallerPrincipal
    protected DatawavePrincipal callerPrincipal;

    @Override
    @PostConstruct
    public void init() {
        // retryCounter is used by super.init() when setting the DatawaveRetryHandler
        retryCounter = metricRegistry.counter("dw.remoteDataDictionary.retries");
        failureCounter = metricRegistry.counter("dw.remoteDataDictionary.failures");
        super.init();
        dataDictReader = objectMapper.readerFor(dataDictionaryType);
    }

    public DataDictionaryBase<?,? extends MetadataFieldBase<?,?>> getDataDictionary(String modelName, String modelTableName, String metadataTableName,
                    String auths) {
        final String bearerHeader = "Bearer " + jwtTokenHandler.createTokenFromUsers(callerPrincipal.getName(), callerPrincipal.getProxiedUsers());
        // @formatter:off
        return executeGetMethodWithRuntimeException("",
                uriBuilder -> {
                    uriBuilder.addParameter("modelName", modelName);
                    uriBuilder.addParameter("modelTableName", modelTableName);
                    uriBuilder.addParameter("metadataTableName", metadataTableName);
                    uriBuilder.addParameter("auths", auths);
                },
                httpGet -> httpGet.setHeader("Authorization", bearerHeader),
                entity -> dataDictReader.readValue(entity.getContent()),
                () -> "getDataDictionary [" + modelName + ", " + modelTableName + ", " + metadataTableName + ", " + auths + "]");
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

    public void setDataDictionaryType(JavaType dataDictionaryType) {
        this.dataDictionaryType = dataDictionaryType;
    }
}
