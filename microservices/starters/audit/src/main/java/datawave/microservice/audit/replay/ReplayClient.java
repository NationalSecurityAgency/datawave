package datawave.microservice.audit.replay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.common.base.Preconditions;

import datawave.microservice.audit.AuditServiceProvider;
import datawave.microservice.audit.replay.status.Status;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.DatawaveUserDetails;

/**
 * Simple rest client for submitting requests to the audit replay service
 *
 * @see Request
 * @see AuditServiceProvider
 */
@Service
@ConditionalOnProperty(name = "audit-client.enabled", havingValue = "true", matchIfMissing = true)
public class ReplayClient {
    
    private static final String DEFAULT_REQUEST_BASE_PATH = "/v1/replay";
    
    private enum ReplayMethod {
        CREATE("create", HttpMethod.POST, String.class),
        CREATE_AND_START("createAndStart", HttpMethod.POST, String.class),
        START("start", HttpMethod.PUT, String.class),
        START_ALL("startAll", HttpMethod.PUT, String.class),
        STATUS("status", HttpMethod.GET, Status.class),
        STATUS_ALL("statusAll", HttpMethod.GET, Status[].class),
        UPDATE("update", HttpMethod.PUT, String.class),
        UPDATE_ALL("updateAll", HttpMethod.PUT, String.class),
        STOP("stop", HttpMethod.PUT, String.class),
        STOP_ALL("stopAll", HttpMethod.PUT, String.class),
        RESUME("resume", HttpMethod.PUT, String.class),
        RESUME_ALL("resumeAll", HttpMethod.PUT, String.class),
        DELETE("delete", HttpMethod.DELETE, String.class),
        DELETE_ALL("deleteAll", HttpMethod.DELETE, String.class);
        
        private String name;
        private HttpMethod httpMethod;
        private Class responseClass;
        
        ReplayMethod(String name, HttpMethod httpMethod, Class responseClass) {
            this.name = name;
            this.httpMethod = httpMethod;
            this.responseClass = responseClass;
        }
        
        public String getName() {
            return name;
        }
        
        public HttpMethod getHttpMethod() {
            return httpMethod;
        }
        
        public Class getResponseClass() {
            return responseClass;
        }
    }
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final AuditServiceProvider serviceProvider;
    private final JWTRestTemplate jwtRestTemplate;
    
    @Autowired
    public ReplayClient(RestTemplateBuilder builder, AuditServiceProvider serviceProvider) {
        this.jwtRestTemplate = builder.build(JWTRestTemplate.class);
        this.serviceProvider = serviceProvider;
    }
    
    /**
     * Creates an audit replay request
     *
     * @param request
     *            Used to set applicable parameters (see below) and to specify the user details for this request. Request Parameters: pathUri (required) The
     *            path where the audit file(s) to be replayed can be found sendRate (optional) The number of messages to send per second replayUnfinishedFiles
     *            (optional) Indicates whether files from an unfinished audit replay should be included
     * @return the audit replay id
     */
    public String create(Request request) {
        validateRequest(request);
        Preconditions.checkNotNull(request.paramMap, "pathUri cannot be null");
        Preconditions.checkNotNull(request.paramMap.get("pathUri"), "pathUri cannot be null");
        
        return (String) submitRequest(ReplayMethod.CREATE, request);
    }
    
    /**
     * Creates an audit replay request, and starts it
     *
     * @param request
     *            Used to set applicable parameters (see below) and to specify the user details for this request. Request Parameters: pathUri (required) The
     *            path where the audit file(s) to be replayed can be found sendRate (optional) The number of messages to send per second replayUnfinishedFiles
     *            (optional) Indicates whether files from an unfinished audit replay should be included
     * @return the audit replay id
     */
    public String createAndStart(Request request) {
        validateRequest(request);
        Preconditions.checkNotNull(request.paramMap, "pathUri cannot be null");
        Preconditions.checkNotNull(request.paramMap.get("pathUri"), "pathUri cannot be null");
        
        return (String) submitRequest(ReplayMethod.CREATE_AND_START, request);
    }
    
    /**
     * Starts an audit replay
     *
     * @param request
     *            Used to set applicable parameters (see below) and to specify the user details for this request. Request Parameters: id (required) The audit
     *            replay id
     * @return status, indicating whether the audit replay was started successfully
     */
    public String start(Request request) {
        validateRequest(request);
        Preconditions.checkNotNull(request.id, "id cannot be null");
        
        return (String) submitRequest(ReplayMethod.START, request);
    }
    
    /**
     * Starts all audit replays
     *
     * @param request
     *            Used to specify the user details for this request.
     * @return status, indicating the number of audit replays which were successfully started
     */
    public String startAll(Request request) {
        validateRequest(request);
        
        return (String) submitRequest(ReplayMethod.START_ALL, request);
    }
    
    /**
     * Gets the status of an audit replay
     *
     * @param request
     *            Used to set applicable parameters (see below) and to specify the user details for this request. Request Parameters: id (required) The audit
     *            replay id
     * @return the status of the audit replay
     */
    public Status status(Request request) {
        validateRequest(request);
        Preconditions.checkNotNull(request.id, "id cannot be null");
        
        return (Status) submitRequest(ReplayMethod.STATUS, request);
    }
    
    /**
     * Lists the status for all audit replays
     *
     * @param request
     *            Used to specify the user details for this request.
     * @return array of statuses for all audit replays
     */
    public Status[] statusAll(Request request) {
        validateRequest(request);
        
        return (Status[]) submitRequest(ReplayMethod.STATUS_ALL, request);
    }
    
    /**
     * Updates an audit replay
     *
     * @param request
     *            Used to set applicable parameters (see below) and to specify the user details for this request. Request Parameters: id (required) The audit
     *            replay id sendRate (required) The number of messages to send per second
     * @return status, indicating whether the update was successful
     */
    public String update(Request request) {
        validateRequest(request);
        Preconditions.checkNotNull(request.id, "id cannot be null");
        Preconditions.checkNotNull(request.paramMap, "sendRate cannot be null");
        Preconditions.checkNotNull(request.paramMap.get("sendRate"), "sendRate cannot be null");
        
        return (String) submitRequest(ReplayMethod.UPDATE, request);
    }
    
    /**
     * Updates all audit replays
     *
     * @param request
     *            Used to set applicable parameters (see below) and to specify the user details for this request. Request Parameters: sendRate (required) The
     *            number of messages to send per second
     * @return status, indicating the number of audit replays which were successfully updated
     */
    public String updateAll(Request request) {
        validateRequest(request);
        Preconditions.checkNotNull(request.paramMap, "sendRate cannot be null");
        Preconditions.checkNotNull(request.paramMap.get("sendRate"), "sendRate cannot be null");
        
        return (String) submitRequest(ReplayMethod.UPDATE_ALL, request);
    }
    
    /**
     * Stops an audit replay
     *
     * @param request
     *            Used to set applicable parameters (see below) and to specify the user details for this request. Request Parameters: id (required) The audit
     *            replay id
     * @return status, indicating whether the audit replay was successfully stopped
     */
    public String stop(Request request) {
        validateRequest(request);
        Preconditions.checkNotNull(request.id, "id cannot be null");
        
        return (String) submitRequest(ReplayMethod.STOP, request);
    }
    
    /**
     * Stops all audit replays
     *
     * @param request
     *            Used to specify the user details for this request.
     * @return status, indicating the number of audit replays which were successfully stopped
     */
    public String stopAll(Request request) {
        validateRequest(request);
        
        return (String) submitRequest(ReplayMethod.STOP_ALL, request);
    }
    
    /**
     * Resumes an audit replay
     *
     * @param request
     *            Used to set applicable parameters (see below) and to specify the user details for this request. Request Parameters: id (required) The audit
     *            replay id
     * @return status, indicating whether the audit replay was successfully resumed
     */
    public String resume(Request request) {
        validateRequest(request);
        Preconditions.checkNotNull(request.id, "id cannot be null");
        
        return (String) submitRequest(ReplayMethod.RESUME, request);
    }
    
    /**
     * Resumes all audit replays
     *
     * @param request
     *            Used to specify the user details for this request.
     * @return status, indicating the number of audit replays which were successfully resumed
     */
    public String resumeAll(Request request) {
        validateRequest(request);
        
        return (String) submitRequest(ReplayMethod.RESUME_ALL, request);
    }
    
    /**
     * Deletes an audit replay
     *
     * @param request
     *            Used to set applicable parameters (see below) and to specify the user details for this request. Request Parameters: id (required) The audit
     *            replay id
     * @return status, indicating whether the audit replay was successfully deleted
     */
    public String delete(Request request) {
        validateRequest(request);
        Preconditions.checkNotNull(request.id, "id cannot be null");
        
        return (String) submitRequest(ReplayMethod.DELETE, request);
    }
    
    /**
     * Deletes all audit replays
     *
     * @param request
     *            Used to specify the user details for this request.
     * @return status, indicating the number of audit replays which were successfully deleted
     */
    public String deleteAll(Request request) {
        validateRequest(request);
        return (String) submitRequest(ReplayMethod.DELETE_ALL, request);
    }
    
    protected void validateRequest(Request request) {
        Preconditions.checkNotNull(request, "request cannot be null");
        Preconditions.checkNotNull(request.datawaveUserDetails, "DatawaveUserDetails cannot be null");
    }
    
    private Object submitRequest(ReplayMethod replayMethod, Request request) {
        log.debug("Submitting {} request: {}", replayMethod.getName(), request.paramMap);
        
        String subPath = (request.id != null) ? request.id + "/" + replayMethod.getName() : replayMethod.getName();
        
        //@formatter:off
        ServiceInstance auditService = serviceProvider.getServiceInstance();
        UriComponents uri = UriComponentsBuilder.fromUri(auditService.getUri())
                .path(auditService.getServiceId() + DEFAULT_REQUEST_BASE_PATH + "/" + subPath)
                .build();

        log.debug("Submitting {} request to {}", replayMethod.getName(), uri);

        ResponseEntity<?> response = jwtRestTemplate.exchange(
                jwtRestTemplate.createRequestEntity(
                        request.datawaveUserDetails,
                        request.paramMap,
                        null,
                        replayMethod.getHttpMethod(), uri),
                replayMethod.getResponseClass()
        );

        if (response.getStatusCode().value() != HttpStatus.OK.value()) {
            String errorMessage = String.format("%s request failed. Http Status: (%s, %s)",
                    replayMethod.getName(),
                    response.getStatusCodeValue(),
                    response.getStatusCode().getReasonPhrase());
            log.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        //@formatter:on
        
        return response.getBody();
    }
    
    /**
     * Replay request for a given query
     *
     * @see Request.Builder
     */
    public static class Request {
        protected DatawaveUserDetails datawaveUserDetails;
        protected String id;
        protected MultiValueMap<String,String> paramMap;
        
        private Request() {
            
        }
        
        /**
         * Used to construct replay requests
         *
         * @param builder
         *            {@link Builder} for the replay request
         */
        protected Request(Builder builder) {
            this.datawaveUserDetails = builder.datawaveUserDetails;
            this.id = builder.id;
            
            MultiValueMap<String,String> paramMap = new LinkedMultiValueMap<>();
            if (builder.pathUri != null) {
                paramMap.add("pathUri", builder.pathUri);
            }
            if (builder.sendRate != null) {
                paramMap.add("sendRate", Long.toString(builder.sendRate));
            }
            if (builder.replayUnfinishedFiles != null) {
                paramMap.add("replayUnfinishedFiles", Boolean.toString(builder.replayUnfinishedFiles));
            }
            if (!paramMap.isEmpty()) {
                this.paramMap = paramMap;
            }
        }
        
        /**
         * Builder for replay requests
         */
        public static class Builder {
            protected DatawaveUserDetails datawaveUserDetails;
            protected String pathUri;
            protected Long sendRate;
            protected Boolean replayUnfinishedFiles;
            protected String id;
            
            public Builder withDatawaveUserDetails(DatawaveUserDetails datawaveUserDetails) {
                this.datawaveUserDetails = datawaveUserDetails;
                return this;
            }
            
            public Builder withPathUri(String pathUri) {
                this.pathUri = pathUri;
                return this;
            }
            
            public Builder withSendRate(Long sendRate) {
                this.sendRate = sendRate;
                return this;
            }
            
            public Builder withReplayUnfinishedFiles(Boolean replayUnfinishedFiles) {
                this.replayUnfinishedFiles = replayUnfinishedFiles;
                return this;
            }
            
            public Builder withId(String id) {
                this.id = id;
                return this;
            }
            
            public Request build() {
                return new Request(this);
            }
        }
    }
}
