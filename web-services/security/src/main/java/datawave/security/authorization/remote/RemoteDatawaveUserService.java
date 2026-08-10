package datawave.security.authorization.remote;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;
import javax.inject.Inject;
import javax.interceptor.Interceptor;
import javax.net.ssl.SSLException;

import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.annotation.Metric;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectReader;

import datawave.configuration.RefreshableScope;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserInfo;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.util.NotEqualPropertyExpressionInterpreter;

/**
 * A {@link CachedDatawaveUserService} that delegates all methods to a remote authorization microservice.
 */
@RefreshableScope
@Alternative
// Make this alternative active for the entire application per the CDI 1.2 specification
@Priority(Interceptor.Priority.APPLICATION)
@Exclude(onExpression = "dw.security.use.remoteuserservice!=true", interpretedBy = NotEqualPropertyExpressionInterpreter.class)
public class RemoteDatawaveUserService extends RemoteHttpService implements CachedDatawaveUserService {
    private ObjectReader datawaveUserReader;
    private ObjectReader datawaveUserListReader;
    private ObjectReader datawaveUserInfoListReader;

    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.useSrvDnsLookup", defaultValue = "false")
    private boolean useSrvDNS;

    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.srvDnsServers", defaultValue = "127.0.0.1")
    private List<String> srvDnsServers;

    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.srvDnsPort", defaultValue = "8600")
    private int srvDnsPort;

    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.scheme", defaultValue = "https")
    private String authServiceScheme;

    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.host", defaultValue = "localhost")
    private String authServiceHost;

    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.port", defaultValue = "8643")
    private int authServicePort;

    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.uri", defaultValue = "/authorization/v1/")
    private String authServiceURI;

    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.maxConnections", defaultValue = "100")
    private int maxConnections;

    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.retryCount", defaultValue = "5")
    private int retryCount;

    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.unavailableRetryCount", defaultValue = "15")
    private int unavailableRetryCount;

    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.unavailableRetryDelayMS", defaultValue = "2000")
    private int unavailableRetryDelay;

    @Inject
    @Metric(name = "dw.remoteDatawaveUserService.retries", absolute = true)
    private Counter retryCounter;

    @Inject
    @Metric(name = "dw.remoteDatawaveUserService.failures", absolute = true)
    private Counter failureCounter;

    @Override
    protected List<Class<? extends IOException>> getUnavailableRetryClasses() {
        return Arrays.asList(ConnectException.class, UnknownHostException.class);
    }

    @Override
    protected List<Class<? extends IOException>> getNonRetriableClasses() {
        return Arrays.asList(SSLException.class);
    }

    @Override
    @Timed(name = "dw.remoteDatawaveUserService.lookup", absolute = true)
    public Collection<DatawaveUser> lookup(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
        final String entitiesHeader = "<" + dns.stream().map(SubjectIssuerDNPair::subjectDN).collect(Collectors.joining("><")) + ">";
        final String issuersHeader = "<" + dns.stream().map(SubjectIssuerDNPair::issuerDN).collect(Collectors.joining("><")) + ">";
        // @formatter:off
        String jwtString = executeGetMethodWithAuthorizationException("authorize",
                uriBuilder -> {},
                httpGet -> {
                    httpGet.setHeader("X-ProxiedEntitiesChain", entitiesHeader);
                    httpGet.setHeader("X-ProxiedIssuersChain", issuersHeader);
                    httpGet.setHeader(HttpHeaders.ACCEPT, ContentType.TEXT_PLAIN.getMimeType());
                },
                EntityUtils::toString,
                () -> "lookup " + dns);
        // @formatter:on
        return jwtTokenHandler.createUsersFromToken(jwtString);
    }

    @Override
    @Timed(name = "dw.remoteDatawaveUserService.reload", absolute = true)
    public Collection<DatawaveUser> reload(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
        Base64.Encoder encoder = Base64.getEncoder();
        // @formatter:off
        return executeGetMethodWithAuthorizationException("admin/reloadUsers",
                // We need to base64 encode each parameter as a work-around since DNs contain
                // commas, which are used as a separator for a multi-valued parameter.
                uriBuilder -> dns.stream()
                                .map(SubjectIssuerDNPair::toString)
                                .map(s -> encoder.encodeToString(s.getBytes()))
                                .forEach(s -> uriBuilder.addParameter("dns", s)),
                httpGet -> {},
                entity -> datawaveUserListReader.readValue(entity.getContent()),
                () -> "reload " + dns);
        // @formatter:on
    }

    @Override
    @Timed(name = "dw.remoteDatawaveUserService.list", absolute = true)
    public DatawaveUser list(String name) {
        // @formatter:off
        return executeGetMethodWithRuntimeException("admin/listUser",
                uriBuilder -> uriBuilder.addParameter("username", name),
                httpGet -> {},
                entity -> datawaveUserReader.readValue(entity.getContent()),
                () -> "list");
        // @formatter:on
    }

    @Override
    @Timed(name = "dw.remoteDatawaveUserService.listAll", absolute = true)
    public Collection<? extends DatawaveUserInfo> listAll() {
        // @formatter:off
        return executeGetMethodWithRuntimeException("admin/listUsers",
                uriBuilder -> {},
                httpGet -> {},
                entity -> datawaveUserInfoListReader.readValue(entity.getContent()),
                () -> "list all users");
        // @formatter:on
    }

    @Override
    @Timed(name = "dw.remoteDatawaveUserService.listMatching", absolute = true)
    public Collection<? extends DatawaveUserInfo> listMatching(String substring) {
        // @formatter:off
        return executeGetMethodWithRuntimeException("admin/listUsersMatching",
                uriBuilder -> uriBuilder.addParameter("substring", substring),
                httpGet -> {},
                entity -> datawaveUserInfoListReader.readValue(entity.getContent()),
                () -> "list all users matching " + substring);
        // @formatter:on
    }

    @Override
    @Timed(name = "dw.remoteDatawaveUserService.evict", absolute = true)
    public String evict(String name) {
        // @formatter:off
        return executeGetMethodWithRuntimeException("admin/evictUser",
                uriBuilder -> uriBuilder.addParameter("username", name),
                httpGet -> httpGet.addHeader(HttpHeaders.ACCEPT, ContentType.TEXT_PLAIN.getMimeType()),
                EntityUtils::toString,
                () -> "evict " + name);
        // @formatter:on
    }

    @Override
    @Timed(name = "dw.remoteDatawaveUserService.evictMatching", absolute = true)
    public String evictMatching(String substring) {
        // @formatter:off
        return executeGetMethodWithRuntimeException("admin/evictUsersMatching",
                uriBuilder -> uriBuilder.addParameter("substring", substring),
                httpGet -> httpGet.addHeader(HttpHeaders.ACCEPT, ContentType.TEXT_PLAIN.getMimeType()),
                EntityUtils::toString,
                () -> "evict users matching " + substring);
        // @formatter:on
    }

    @Override
    @Timed(name = "dw.remoteDatawaveUserService.evictAll", absolute = true)
    public String evictAll() {
        // @formatter:off
        return executeGetMethodWithRuntimeException("admin/evictAll",
                b -> {},
                httpGet -> httpGet.addHeader(HttpHeaders.ACCEPT, ContentType.TEXT_PLAIN.getMimeType()),
                EntityUtils::toString,
                () -> "evict all users");
        // @formatter:on
    }

    @PostConstruct
    protected void init() {
        super.init();
        datawaveUserReader = objectMapper.readerFor(DatawaveUser.class);
        datawaveUserListReader = objectMapper.readerFor(objectMapper.getTypeFactory().constructCollectionType(Collection.class, DatawaveUser.class));
        datawaveUserInfoListReader = objectMapper.readerFor(objectMapper.getTypeFactory().constructCollectionType(Collection.class, DatawaveUserInfo.class));
    }

    @Override
    protected String serviceHost() {
        return authServiceHost;
    }

    @Override
    protected int servicePort() {
        return authServicePort;
    }

    @Override
    protected String serviceURI() {
        return authServiceURI;
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
        return authServiceScheme;
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
