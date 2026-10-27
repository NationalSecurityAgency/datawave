package datawave.microservice.annotation.client;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClient;

import datawave.webservice.result.VoidResponse;
import reactor.core.publisher.Mono;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class AnnotationTestClient {

    public static final int DEFAULT_PORT = 9543;

    protected String path = "/annotation/v1";
    protected int port = DEFAULT_PORT;

    @Autowired
    private WebClient.Builder webClientBuilder;

    public void testAnnotationServiceCall() {
        WebClient webClient = webClientBuilder.baseUrl("https://localhost:" + port + path).build();
        ResponseEntity<VoidResponse> responseEntity = webClient.get().uri("/testSingleQueryException").retrieve()
                        .onStatus(HttpStatus::isError, response -> Mono.empty()).toEntity(VoidResponse.class).block();
    }
}
