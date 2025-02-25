package datawave.microservice.authorization.jwt;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.authorization.JWTTokenHandler;

/**
 * An extended version of {@link RestTemplate} that provides convenience methods to ensure the JWT is passed with a request.
 */
public class JWTRestTemplate extends RestTemplate {
    private JWTTokenHandler jwtTokenHandler;
    
    public void setJwtTokenHandler(JWTTokenHandler jwtTokenHandler) {
        this.jwtTokenHandler = jwtTokenHandler;
    }
    
    public <T> RequestEntity<T> createRequestEntity(DatawaveUserDetails currentUser, HttpMethod method, UriComponents uri) {
        return createRequestEntity(currentUser, null, null, method, uri);
    }
    
    public <T> RequestEntity<T> createRequestEntity(DatawaveUserDetails currentUser, T body, MultiValueMap<String,String> additionalHeaders, HttpMethod method,
                    UriComponents uri) {
        String token = jwtTokenHandler.createTokenFromUsers(currentUser.getUsername(), currentUser.getProxiedUsers());
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        if (additionalHeaders != null) {
            additionalHeaders.forEach(headers::put);
        }
        return new RequestEntity<>(body, headers, method, uri.toUri());
    }
    
    public <T> ResponseEntity<T> exchange(DatawaveUserDetails currentUser, HttpMethod method, UriComponents uri, Class<T> responseType)
                    throws RestClientException {
        RequestEntity<T> requestEntity = createRequestEntity(currentUser, method, uri);
        return exchange(requestEntity, responseType);
    }
}
