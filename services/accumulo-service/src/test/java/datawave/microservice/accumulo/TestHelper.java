package datawave.microservice.accumulo;

import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.junit.Assert.assertEquals;

/**
 * Utility class for simplifying most of the typical REST API test functions
 */
public class TestHelper {
    
    public static final SubjectIssuerDNPair DEFAULT_USER_DN = SubjectIssuerDNPair.of("userDn", "issuerDn");
    
    private final JWTRestTemplate jwtRestTemplate;
    private final ProxiedUserDetails defaultUserDetails;
    private final String webServiceBasePath;
    private final int webServicePort;
    
    public TestHelper(JWTRestTemplate jwtRestTemplate, ProxiedUserDetails defaultUserDetails, int webServicePort, String webServiceBasePath) {
        this.jwtRestTemplate = jwtRestTemplate;
        this.defaultUserDetails = defaultUserDetails;
        this.webServiceBasePath = webServiceBasePath;
        this.webServicePort = webServicePort;
    }
    
    public <T> T assert200Status(RequestEntity<?> request, Class<T> responseType) {
        ResponseEntity<T> entity = jwtRestTemplate.exchange(request, responseType);
        assertEquals("Request to '" + request.getUrl() + "' did not return 200 status", HttpStatus.OK, entity.getStatusCode());
        return entity.getBody();
    }
    
    public RequestEntity<?> createGetRequest(String path) {
        return createGetRequest(defaultUserDetails, path);
    }
    
    public RequestEntity<?> createGetRequest(ProxiedUserDetails userDetails, String path) {
        return jwtRestTemplate.createRequestEntity(userDetails, null, null, HttpMethod.GET, getUri(path));
    }
    
    public <T> RequestEntity<T> createPostRequest(String path, T requestBody) {
        return createPostRequest(defaultUserDetails, path, requestBody);
    }
    
    public <T> RequestEntity<T> createPutRequest(String path, T requestBody) {
        return createPutRequest(defaultUserDetails, path, requestBody);
    }
    
    public <T> RequestEntity<T> createPostRequest(ProxiedUserDetails userDetails, String path, T requestBody) {
        return createRequest(userDetails, path, requestBody, HttpMethod.POST);
    }
    
    public <T> RequestEntity<T> createPutRequest(ProxiedUserDetails userDetails, String path, T requestBody) {
        return createRequest(userDetails, path, requestBody, HttpMethod.PUT);
    }
    
    public <T> RequestEntity<T> createRequest(ProxiedUserDetails userDetails, String path, T requestBody, HttpMethod method) {
        return jwtRestTemplate.createRequestEntity(userDetails, requestBody, null, method, getUri(path));
    }
    
    public UriComponents getUri(String testPath) {
        //@formatter:off
        return UriComponentsBuilder.newInstance()
                .scheme("https")
                .host("localhost")
                .port(webServicePort)
                .path(webServiceBasePath + testPath)
                .build();
        //@formatter:on
    }
    
    public static ProxiedUserDetails userDetails(Collection<String> assignedRoles, Collection<String> assignedAuths) {
        return userDetails(DEFAULT_USER_DN, assignedRoles, assignedAuths);
    }
    
    public static ProxiedUserDetails userDetails(SubjectIssuerDNPair dn, Collection<String> assignedRoles, Collection<String> assignedAuths) {
        DatawaveUser dwUser = new DatawaveUser(dn, USER, assignedAuths, assignedRoles, null, System.currentTimeMillis());
        return new ProxiedUserDetails(Collections.singleton(dwUser), dwUser.getCreationTime());
    }
    
    public static String queryString(String... params) {
        StringBuilder sb = new StringBuilder();
        Arrays.stream(params).forEach(s -> sb.append(s).append("&"));
        return sb.toString();
    }
}
