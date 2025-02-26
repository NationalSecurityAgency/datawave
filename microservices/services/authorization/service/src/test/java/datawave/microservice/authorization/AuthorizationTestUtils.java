package datawave.microservice.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import datawave.microservice.authorization.preauth.ProxiedEntityX509Filter;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.authorization.JWTTokenHandler;

public class AuthorizationTestUtils {
    
    private JWTTokenHandler jwtTokenHandler;
    private RestTemplate restTemplate;
    private String scheme;
    private int webServicePort;
    
    public AuthorizationTestUtils(JWTTokenHandler jwtTokenHandler, RestTemplate restTemplate, String scheme, int webServicePort) {
        this.jwtTokenHandler = jwtTokenHandler;
        this.restTemplate = restTemplate;
        this.scheme = scheme;
        this.webServicePort = webServicePort;
    }
    
    public void testAdminMethodFailure(DatawaveUserDetails unauthUser, String path, String query) throws Exception {
        UriComponents uri = UriComponentsBuilder.newInstance().scheme(scheme).host("localhost").port(webServicePort).path(path).query(query).build();
        try {
            RequestEntity requestEntity = createRequestEntity(null, unauthUser, HttpMethod.GET, uri);
            restTemplate.exchange(requestEntity, String.class);
            fail("Non-admin request to " + uri + " shouldn't have been allowed.");
        } catch (HttpClientErrorException e) {
            assertEquals(403, e.getRawStatusCode());
            assertEquals("403 Forbidden: \"<html><head><title>Error</title></head><body>Forbidden</body></html>\"", e.getMessage());
        }
    }
    
    public void testAdminMethodSuccess(DatawaveUserDetails authUser, String path, String query) throws Exception {
        UriComponents uri = UriComponentsBuilder.newInstance().scheme(scheme).host("localhost").port(webServicePort).path(path).query(query).build();
        RequestEntity requestEntity = createRequestEntity(null, authUser, HttpMethod.GET, uri);
        ResponseEntity<String> entity = restTemplate.exchange(requestEntity, String.class);
        assertEquals(HttpStatus.OK, entity.getStatusCode(), "Authorizaed admin request to " + uri + " did not return a 200.");
    }
    
    public void testAuthorizeMethodFailure(DatawaveUserDetails unauthUser, String path, boolean useTrustedHeader, boolean useJWT) throws Exception {
        UriComponents uri = UriComponentsBuilder.newInstance().scheme(scheme).host("localhost").port(webServicePort).path(path).build();
        try {
            RequestEntity requestEntity;
            DatawaveUserDetails trustedHeaderUser = useTrustedHeader ? unauthUser : null;
            if (useJWT) {
                requestEntity = createRequestEntity(trustedHeaderUser, unauthUser, HttpMethod.GET, uri);
            } else {
                requestEntity = createRequestEntity(trustedHeaderUser, null, HttpMethod.GET, uri);
            }
            restTemplate.exchange(requestEntity, String.class);
            fail("Non-allowed-caller request to " + uri + " shouldn't have been allowed.");
        } catch (HttpClientErrorException e) {
            assertEquals(403, e.getRawStatusCode());
            assertTrue(e.getMessage().startsWith("403 Forbidden"));
        }
    }
    
    public void testAuthorizeMethodSuccess(DatawaveUserDetails authUser, String path, boolean useTrustedHeader, boolean useJWT) throws Exception {
        UriComponents uri = UriComponentsBuilder.newInstance().scheme(scheme).host("localhost").port(webServicePort).path(path).build();
        RequestEntity requestEntity;
        DatawaveUserDetails trustedHeaderUser = useTrustedHeader ? authUser : null;
        if (useJWT) {
            requestEntity = createRequestEntity(trustedHeaderUser, authUser, HttpMethod.GET, uri);
        } else {
            requestEntity = createRequestEntity(trustedHeaderUser, null, HttpMethod.GET, uri);
        }
        ResponseEntity<String> responseEntity = restTemplate.exchange(requestEntity, String.class);
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode(), "Authorized request to " + uri + " did not return a 200.");
    }
    
    public <T> RequestEntity<T> createRequestEntity(DatawaveUserDetails trustedUser, DatawaveUserDetails jwtUser, HttpMethod method, UriComponents uri) {
        
        HttpHeaders headers = new HttpHeaders();
        if (this.jwtTokenHandler != null && jwtUser != null) {
            String token = jwtTokenHandler.createTokenFromUsers(jwtUser.getUsername(), jwtUser.getProxiedUsers());
            headers.add("Authorization", "Bearer " + token);
        }
        if (trustedUser != null) {
            headers.add(ProxiedEntityX509Filter.SUBJECT_DN_HEADER, trustedUser.getPrimaryUser().getDn().subjectDN());
            headers.add(ProxiedEntityX509Filter.ISSUER_DN_HEADER, trustedUser.getPrimaryUser().getDn().issuerDN());
        }
        return new RequestEntity(null, headers, method, uri.toUri());
    }
    
    public String getScheme() {
        return scheme;
    }
}
