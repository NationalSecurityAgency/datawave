package datawave.microservice.querymetric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import datawave.microservice.authorization.preauth.ProxiedEntityX509Filter;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.querymetric.config.QueryMetricClientProperties;
import datawave.microservice.querymetric.config.QueryMetricClientProperties.Retry;
import datawave.microservice.querymetric.config.QueryMetricTransportType;
import datawave.microservice.querymetric.function.QueryMetricSupplier;
import datawave.security.authorization.JWTTokenHandler;
import datawave.webservice.result.VoidResponse;

/**
 * Rest and spring cloud stream client for submitting query metric updates to the query metric service
 *
 * @see Request
 */
@Service
@ConditionalOnProperty(name = "datawave.query.metric.client.enabled", havingValue = "true", matchIfMissing = true)
public class QueryMetricClient {
    // Note: This must match 'confirmAckChannel' in the service configuration. Default set in bootstrap.yml.
    public static final String CONFIRM_ACK_CHANNEL = "confirmAckChannel";
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private RestTemplate restTemplate;
    
    private QueryMetricClientProperties queryMetricClientProperties;
    
    private QueryMetricSupplier queryMetricSupplier;
    
    private ObjectMapper objectMapper;
    
    private JWTTokenHandler jwtTokenHandler;
    
    private static final Map<String,CountDownLatch> correlationLatchMap = new ConcurrentHashMap<>();
    
    public QueryMetricClient(RestTemplateBuilder restTemplateBuilder, QueryMetricClientProperties queryMetricClientProperties,
                    @Autowired(required = false) QueryMetricSupplier queryMetricSupplier, ObjectMapper objectMapper,
                    @Autowired(required = false) JWTTokenHandler jwtTokenHandler) {
        this.queryMetricClientProperties = queryMetricClientProperties;
        this.queryMetricSupplier = queryMetricSupplier;
        
        this.objectMapper = objectMapper;
        this.restTemplate = restTemplateBuilder.build();
        this.jwtTokenHandler = jwtTokenHandler;
    }
    
    public void submit(Request request) throws Exception {
        submit(request, queryMetricClientProperties.getTransport());
    }
    
    public void submit(Request request, QueryMetricTransportType transportType) throws Exception {
        if (request.metrics == null || request.metrics.isEmpty()) {
            throw new IllegalArgumentException("Request must contain a query metric");
        }
        if (request.metricType == null) {
            throw new IllegalArgumentException("Request must contain a query metric type");
        }
        if (transportType == QueryMetricTransportType.MESSAGE) {
            submitViaMessage(request);
        } else {
            submitViaRest(request);
        }
    }
    
    private void submitViaMessage(Request request) {
        if (!updateMetrics(request.metrics.stream().map(m -> new QueryMetricUpdate(m, request.metricType)).collect(Collectors.toList()))) {
            throw new RuntimeException("Unable to process query metric update");
        }
    }
    
    /**
     * Receives producer confirm acks, and disengages the latch associated with the given correlation ID.
     *
     * @param message
     *            the confirmation ack message
     */
    @ServiceActivator(inputChannel = CONFIRM_ACK_CHANNEL)
    public void processConfirmAck(Message<?> message) {
        if (queryMetricClientProperties.isConfirmAckEnabled()) {
            Object headerObj = message.getHeaders().get(IntegrationMessageHeaderAccessor.CORRELATION_ID);
            
            if (headerObj != null) {
                String correlationId = headerObj.toString();
                if (correlationLatchMap.containsKey(correlationId)) {
                    correlationLatchMap.get(correlationId).countDown();
                } else {
                    log.warn("Unable to decrement latch for ID [{}]", correlationId);
                }
            } else {
                log.warn("No correlation ID found in confirm ack message");
            }
        }
    }
    
    private boolean updateMetrics(List<QueryMetricUpdate> updates) {
        Map<String,QueryMetricUpdate> updatesById = new LinkedHashMap<>();
        
        boolean success;
        final long updateStartTime = System.currentTimeMillis();
        long currentTime;
        int attempts = 0;
        
        Retry retry = queryMetricClientProperties.getRetry();
        
        List<QueryMetricUpdate> failedConfirmAck = new ArrayList<>(updates.size());
        do {
            if (attempts++ > 0) {
                try {
                    Thread.sleep(retry.getBackoffIntervalMillis());
                } catch (InterruptedException e) {
                    // Ignore -- we'll just end up retrying a little too fast
                }
                
                // perform some retry upkeep
                updates.addAll(failedConfirmAck);
                failedConfirmAck.clear();
            }
            
            if (log.isDebugEnabled()) {
                log.debug("Bulk update attempt {} of {}", attempts, retry.getMaxAttempts());
            }
            
            // send all of the remaining metric updates
            success = sendMessages(updates, updatesById) && awaitConfirmAcks(updatesById, failedConfirmAck);
            currentTime = System.currentTimeMillis();
        } while (!success && (currentTime - updateStartTime) < retry.getFailTimeoutMillis() && attempts < retry.getMaxAttempts());
        
        if (!success) {
            log.warn("Bulk update failed. {attempts = {}, elapsedMillis = {}}", attempts, (currentTime - updateStartTime));
        } else {
            log.debug("Bulk update successful. {attempts = {}, elapsedMillis = {}}", attempts, (currentTime - updateStartTime));
        }
        
        return success;
    }
    
    private boolean updateMetric(QueryMetricUpdate update) {
        return updateMetrics(Lists.newArrayList(update));
    }
    
    /**
     * Passes query metric messages to the messaging infrastructure.
     *
     * @param updates
     *            The query metric updates to be sent, not null
     * @param updatesById
     *            A map that will be populated with the correlation ids and associated metric updates, not null
     * @return true if all messages were successfully sent, false otherwise
     */
    private boolean sendMessages(List<QueryMetricUpdate> updates, Map<String,QueryMetricUpdate> updatesById) {
        
        List<QueryMetricUpdate> failedSend = new ArrayList<>(updates.size());
        
        boolean success = true;
        // send all of the remaining metric updates
        for (QueryMetricUpdate update : updates) {
            String correlationId = UUID.randomUUID().toString();
            if (sendMessage(correlationId, update)) {
                if (queryMetricClientProperties.isConfirmAckEnabled()) {
                    updatesById.put(correlationId, update);
                }
            } else {
                // if it failed, add it to the failed list
                failedSend.add(update);
                success = false;
            }
        }
        
        updates.retainAll(failedSend);
        
        return success;
    }
    
    private boolean sendMessage(String correlationId, QueryMetricUpdate update) {
        boolean success = false;
        if (queryMetricSupplier.send(MessageBuilder.withPayload(update).setCorrelationId(correlationId).build())) {
            success = true;
            if (queryMetricClientProperties.isConfirmAckEnabled()) {
                correlationLatchMap.put(correlationId, new CountDownLatch(1));
            }
        }
        return success;
    }
    
    /**
     * Waits for the producer confirm acks to be received for the updates that were sent. If a producer confirm ack is not received within the specified amount
     * of time, a 500 Internal Server Error will be returned to the caller.
     *
     * @param updatesById
     *            A map of query metric updates keyed by their correlation id, not null
     * @param failedConfirmAck
     *            A list that will be populated with the failed metric updates, not null
     * @return true if all confirm acks were successfully received, false otherwise
     */
    private boolean awaitConfirmAcks(Map<String,QueryMetricUpdate> updatesById, List<QueryMetricUpdate> failedConfirmAck) {
        boolean success = true;
        // wait for the confirm acks only after all sends are successful
        if (queryMetricClientProperties.isConfirmAckEnabled()) {
            for (String correlationId : new HashSet<>(updatesById.keySet())) {
                if (!awaitConfirmAck(correlationId)) {
                    failedConfirmAck.add(updatesById.remove(correlationId));
                    success = false;
                }
            }
        }
        return success;
    }
    
    private boolean awaitConfirmAck(String correlationId) {
        boolean success = false;
        if (queryMetricClientProperties.isConfirmAckEnabled()) {
            try {
                success = correlationLatchMap.get(correlationId).await(queryMetricClientProperties.getConfirmAckTimeoutMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("Interrupted waiting for confirm ack {}", correlationId);
            } finally {
                correlationLatchMap.remove(correlationId);
            }
        }
        return success;
    }
    
    private void submitViaRest(Request request) throws Exception {
        if (request.user == null && request.trustedUser == null) {
            throw new IllegalArgumentException("Request must contain either user or trustedUser to use HTTP/HTTPS transport");
        }
        QueryMetricTransportType transportType = queryMetricClientProperties.getTransport();
        if (this.jwtTokenHandler == null) {
            throw new IllegalArgumentException("jwtTokenHandler can not be null with transportType " + transportType.toString());
        }
        QueryMetricType metricType = request.metricType;
        String scheme = queryMetricClientProperties.getScheme();
        String host = this.queryMetricClientProperties.getHost();
        int port = this.queryMetricClientProperties.getPort();
        String url;
        Object metricObject;
        if (request.metrics.size() == 1) {
            url = this.queryMetricClientProperties.getUpdateMetricUrl();
            metricObject = request.metrics.get(0);
        } else {
            url = this.queryMetricClientProperties.getUpdateMetricsUrl();
            metricObject = request.metrics;
        }
        
        HttpEntity requestEntity = createRequestEntity(request.user, request.trustedUser, metricObject);
        // @formatter:off
        UriComponents metricUpdateUri = UriComponentsBuilder.newInstance()
                .scheme(scheme)
                .host(host)
                .port(port)
                .path(url)
                .queryParam("metricType", metricType)
                .build();
        // @formatter:on
        restTemplate.postForEntity(metricUpdateUri.toUri(), requestEntity, VoidResponse.class);
    }
    
    protected HttpEntity createRequestEntity(DatawaveUserDetails user, DatawaveUserDetails trustedUser, Object body) throws JsonProcessingException {
        
        HttpHeaders headers = new HttpHeaders();
        if (this.jwtTokenHandler != null && user != null) {
            String token = this.jwtTokenHandler.createTokenFromUsers(user.getUsername(), user.getProxiedUsers());
            headers.add("Authorization", "Bearer " + token);
        }
        if (trustedUser != null) {
            headers.add(ProxiedEntityX509Filter.SUBJECT_DN_HEADER, trustedUser.getPrimaryUser().getDn().subjectDN());
            headers.add(ProxiedEntityX509Filter.ISSUER_DN_HEADER, trustedUser.getPrimaryUser().getDn().issuerDN());
        }
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        return new HttpEntity<>(objectMapper.writeValueAsString(body), headers);
    }
    
    /**
     * Query metric update request
     *
     * @see Request.Builder
     */
    public static class Request {
        
        protected List<BaseQueryMetric> metrics;
        protected QueryMetricType metricType;
        protected DatawaveUserDetails user;
        protected DatawaveUserDetails trustedUser;
        
        private Request() {}
        
        /**
         * Constructs a query metric update request
         *
         * @param b
         *            {@link Builder} for the query metric update request
         */
        protected Request(Builder b) {
            this.metrics = b.metrics;
            this.metricType = b.metricType;
            this.user = b.user;
            this.trustedUser = b.trustedUser;
        }
        
        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this).toString();
        }
        
        /**
         * Builder for base audit requests
         */
        public static class Builder {
            
            protected List<BaseQueryMetric> metrics;
            protected QueryMetricType metricType;
            protected DatawaveUserDetails user;
            protected DatawaveUserDetails trustedUser;
            
            public Builder withMetricType(QueryMetricType metricType) {
                this.metricType = metricType;
                return this;
            }
            
            public Builder withMetric(BaseQueryMetric metric) {
                this.metrics = Collections.singletonList(metric);
                return this;
            }
            
            public Builder withMetrics(List<BaseQueryMetric> metrics) {
                this.metrics = metrics;
                return this;
            }
            
            public Builder withUser(DatawaveUserDetails user) {
                this.user = user;
                return this;
            }
            
            public Builder withTrustedUser(DatawaveUserDetails trustedUser) {
                this.trustedUser = trustedUser;
                return this;
            }
            
            public Request build() {
                return new Request(this);
            }
        }
    }
}
