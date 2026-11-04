package datawave.microservice.audit.health.rabbit;

import java.net.URI;
import java.util.Map;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.rabbitmq.http.client.HttpLayer;
import com.rabbitmq.http.client.ParameterizedTypeReference;

/**
 * An {@link HttpLayer} backed by a Spring {@link RestTemplate}, so tests can drive the RabbitMQ management {@link com.rabbitmq.http.client.Client} with a
 * {@link org.springframework.test.web.client.MockRestServiceServer}. The 5.x client uses the JDK HttpClient by default, which a mock server can't intercept.
 * Like the default layer, a 404 on GET returns null.
 */
public class RestTemplateHttpLayer implements HttpLayer {
    private final RestTemplate restTemplate;

    public RestTemplateHttpLayer(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @Override
    public <T> T get(URI uri, Class<T> responseClass) {
        try {
            return restTemplate.getForObject(uri, responseClass);
        } catch (HttpClientErrorException.NotFound e) {
            return null;
        }
    }

    @Override
    public <T> T get(URI uri, ParameterizedTypeReference<T> typeReference) {
        try {
            return restTemplate.exchange(uri, HttpMethod.GET, null, org.springframework.core.ParameterizedTypeReference.<T> forType(typeReference.getType()))
                            .getBody();
        } catch (HttpClientErrorException.NotFound e) {
            return null;
        }
    }

    @Override
    public <T> T post(URI uri, Object requestBody, Class<T> responseClass) {
        // the client passes a null response class for calls with no response body (e.g., bindQueue)
        if (responseClass == null) {
            restTemplate.postForLocation(uri, requestBody);
            return null;
        }
        return restTemplate.postForObject(uri, requestBody, responseClass);
    }

    @Override
    public void put(URI uri, Object requestBody) {
        restTemplate.put(uri, requestBody);
    }

    @Override
    public void delete(URI uri, Map<String,String> headers) {
        HttpHeaders httpHeaders = new HttpHeaders();
        if (headers != null) {
            headers.forEach(httpHeaders::add);
        }
        restTemplate.exchange(uri, HttpMethod.DELETE, new HttpEntity<>(httpHeaders), Void.class);
    }
}
