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
import javax.inject.Inject;
import javax.net.ssl.SSLException;

import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.annotation.Metric;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectReader;

import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserInfo;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.remote.RemoteHttpService;

/**
 * A {@link CachedDatawaveUserService} that delegates all methods to a remote authorization microservice.
 */
public class RemoteDatawaveUserService extends RemoteHttpService implements CachedDatawaveUserService {
    private ObjectReader datawaveUserReader;
    private ObjectReader datawaveUserListReader;
    private ObjectReader datawaveUserInfoListReader;

    private String authServiceURI = "/authorization/v1/";

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
    protected String serviceURI() {
        return authServiceURI;
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
