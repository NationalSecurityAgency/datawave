package datawave.webservice.datadictionary;

import java.net.URI;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.http.client.utils.URIBuilder;
import org.xbill.DNS.TextParseException;

import com.codahale.metrics.Counter;
import com.codahale.metrics.annotation.Metric;
import com.fasterxml.jackson.core.type.TypeReference;
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

    @Inject
    @ConfigProperty(name = "dw.remoteDictionary.useSrvDnsLookup", defaultValue = "false")
    private boolean useSrvDNS;

    @Inject
    @ConfigProperty(name = "dw.remoteDictionary.srvDnsServers", defaultValue = "127.0.0.1")
    private List<String> srvDnsServers;

    @Inject
    @ConfigProperty(name = "dw.remoteDictionary.srvDnsPort", defaultValue = "8600")
    private int srvDnsPort;

    @Inject
    @ConfigProperty(name = "dw.remoteDictionary.scheme", defaultValue = "https")
    private String dictServiceScheme;

    @Inject
    @ConfigProperty(name = "dw.remoteDictionary.host", defaultValue = "localhost")
    private String dictServiceHost;

    @Inject
    @ConfigProperty(name = "dw.remoteDictionary.port", defaultValue = "8843")
    private int dictServicePort;

    @Inject
    @ConfigProperty(name = "dw.remoteDictionary.useConfiguredURIForRedirect", defaultValue = "false")
    private boolean useConfiguredURIForRedirect;

    @Inject
    @ConfigProperty(name = "dw.remoteDictionary.data.uri", defaultValue = "/dictionary/data/v1/")
    private String dictServiceURI;

    @Inject
    @ConfigProperty(name = "dw.remoteDictionary.maxConnections", defaultValue = "100")
    private int maxConnections;

    @Inject
    @ConfigProperty(name = "dw.remoteDictionary.retryCount", defaultValue = "5")
    private int retryCount;

    @Inject
    @ConfigProperty(name = "dw.remoteDictionary.unavailableRetryCount", defaultValue = "15")
    private int unavailableRetryCount;

    @Inject
    @ConfigProperty(name = "dw.remoteDictionary.unavailableRetryDelayMS", defaultValue = "2000")
    private int unavailableRetryDelay;

    @Inject
    @Metric(name = "dw.remoteDictionary.retries", absolute = true)
    private Counter retryCounter;

    @Inject
    @Metric(name = "dw.remoteDictionary.failures", absolute = true)
    private Counter failureCounter;

    @Inject
    @CallerPrincipal
    protected DatawavePrincipal callerPrincipal;

    @Inject
    @DataDictionaryType
    protected TypeReference<? extends DataDictionaryBase<?,? extends MetadataFieldBase<?,?>>> dataDictionaryType;

    @Override
    @PostConstruct
    public void init() {
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

    @Override
    protected String serviceHost() {
        return dictServiceHost;
    }

    @Override
    protected int servicePort() {
        return dictServicePort;
    }

    @Override
    protected String serviceURI() {
        return dictServiceURI;
    }

    @Override
    protected boolean useSrvDns() {
        return useSrvDNS;
    }

    @Override
    protected List<String> srvDnsServers() {
        return srvDnsServers;
    }

    @Override
    protected int srvDnsPort() {
        return srvDnsPort;
    }

    @Override
    protected String serviceScheme() {
        return dictServiceScheme;
    }

    @Override
    protected int maxConnections() {
        return maxConnections;
    }

    @Override
    protected int retryCount() {
        return retryCount;
    }

    @Override
    protected int unavailableRetryCount() {
        return unavailableRetryCount;
    }

    @Override
    protected int unavailableRetryDelay() {
        return unavailableRetryDelay;
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
