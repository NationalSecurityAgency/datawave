package datawave.microservice.accumulo;

import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collection;
import java.util.Collections;

import org.junit.jupiter.api.function.Executable;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;

/**
 * Utility class for simplifying most of the typical REST API test functions
 */
public class TestHelper {
    
    public static final SubjectIssuerDNPair DEFAULT_USER_DN = SubjectIssuerDNPair.of("userDn", "issuerDn");
    
    private final JWTRestTemplate jwtRestTemplate;
    private final DatawaveUserDetails defaultUserDetails;
    private final String webServiceBasePath;
    private final int webServicePort;
    
    public TestHelper(JWTRestTemplate jwtRestTemplate, DatawaveUserDetails defaultUserDetails, int webServicePort, String webServiceBasePath) {
        this.jwtRestTemplate = jwtRestTemplate;
        this.defaultUserDetails = defaultUserDetails;
        this.webServiceBasePath = webServiceBasePath;
        this.webServicePort = webServicePort;
    }
    
    public <T> T assert200Status(RequestEntity<?> request, Class<T> responseType) {
        ResponseEntity<T> entity = jwtRestTemplate.exchange(request, responseType);
        assertEquals(HttpStatus.OK, entity.getStatusCode(), "Request to '" + request.getUrl() + "' did not return 200 status");
        return entity.getBody();
    }
    
    public RequestEntity<?> createGetRequest(String path) {
        return createGetRequest(defaultUserDetails, path);
    }
    
    public RequestEntity<?> createGetRequest(DatawaveUserDetails userDetails, String path) {
        return jwtRestTemplate.createRequestEntity(userDetails, null, null, HttpMethod.GET, getUri(path));
    }
    
    public <T> RequestEntity<T> createPostRequest(String path, T requestBody) {
        return createPostRequest(defaultUserDetails, path, requestBody);
    }
    
    public <T> RequestEntity<T> createPutRequest(String path, T requestBody) {
        return createPutRequest(defaultUserDetails, path, requestBody);
    }
    
    public <T> RequestEntity<T> createPostRequest(DatawaveUserDetails userDetails, String path, T requestBody) {
        return createRequest(userDetails, path, requestBody, HttpMethod.POST);
    }
    
    public <T> RequestEntity<T> createPutRequest(DatawaveUserDetails userDetails, String path, T requestBody) {
        return createRequest(userDetails, path, requestBody, HttpMethod.PUT);
    }
    
    public <T> RequestEntity<T> createRequest(DatawaveUserDetails userDetails, String path, T requestBody, HttpMethod method) {
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
    
    public static DatawaveUserDetails userDetails(Collection<String> assignedRoles, Collection<String> assignedAuths) {
        return userDetails(DEFAULT_USER_DN, assignedRoles, assignedAuths);
    }
    
    public static DatawaveUserDetails userDetails(SubjectIssuerDNPair dn, Collection<String> assignedRoles, Collection<String> assignedAuths) {
        DatawaveUser dwUser = new DatawaveUser(dn, USER, assignedAuths, assignedRoles, null, System.currentTimeMillis());
        return new DatawaveUserDetails(Collections.singleton(dwUser), dwUser.getCreationTime());
    }
    
    public static <T extends HttpStatusCodeException> void assertHttpException(Class<T> exceptionClass, int statusCode, Executable executable) {
        HttpStatusCodeException thrown = assertThrows(exceptionClass, executable);
        assertThat("Unexpected HTTP status code", thrown.getRawStatusCode(), equalTo(statusCode));
    }
    
    public static <T extends Throwable> void assertExceptionMessage(Class<T> exceptionClass, String message, Executable executable) {
        T thrown = assertThrows(exceptionClass, executable);
        assertThat(thrown.getMessage(), containsString(message));
    }
}
