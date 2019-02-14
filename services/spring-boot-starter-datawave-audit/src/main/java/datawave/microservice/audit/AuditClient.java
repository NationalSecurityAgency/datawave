package datawave.microservice.audit;

import com.google.common.base.Preconditions;
import datawave.marking.SecurityMarking;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.audit.Auditor.AuditType;
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

/**
 * Simple rest client for submitting requests to the audit service
 *
 * @see Request
 * @see AuditServiceProvider
 */
@Service
@ConditionalOnProperty(name = "audit-client.enabled", havingValue = "true", matchIfMissing = true)
public class AuditClient {
    
    private static final String DEFAULT_REQUEST_PATH = "/v1/audit";
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final AuditServiceProvider serviceProvider;
    private final JWTRestTemplate jwtRestTemplate;
    
    @Autowired
    public AuditClient(RestTemplateBuilder builder, AuditServiceProvider serviceProvider) {
        this.jwtRestTemplate = builder.build(JWTRestTemplate.class);
        this.serviceProvider = serviceProvider;
    }
    
    public void submit(Request request) {
        submit(request, DEFAULT_REQUEST_PATH);
    }
    
    public void submit(Request request, String requestPath) {
        
        Preconditions.checkNotNull(request, "request cannot be null");
        Preconditions.checkNotNull(requestPath, "requestPath cannot be null");
        
        log.debug("Submitting audit request: {}", request.getAuditParameters());
        
        if (AuditType.NONE.equals(request.getAuditType())) {
            if (serviceProvider.getProperties().isSuppressAuditTypeNone()) {
                log.debug("Audit request with AuditType == {} was suppressed", AuditType.NONE);
                return;
            }
        }
        
        //@formatter:off
        ServiceInstance auditService = serviceProvider.getServiceInstance();
        UriComponents uri = UriComponentsBuilder.fromUri(auditService.getUri())
            .path(auditService.getServiceId() + requestPath)
            .queryParams(request.getAuditParametersAsMap())
            .build();

        log.debug("Submitting audit request to {}", uri);

        ResponseEntity<String> response = jwtRestTemplate.exchange(
            request.getUserDetails(), HttpMethod.POST, uri, String.class);

        if (response.getStatusCode().value() != HttpStatus.OK.value()) {
            String errorMessage = String.format("Audit request failed. Http Status: (%s, %s)",
                    response.getStatusCodeValue(),
                    response.getStatusCode().getReasonPhrase());
            log.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        //@formatter:on
    }
    
    /**
     * Audit request for a given query
     *
     * @see Request.Builder
     */
    public static class Request {
        
        static final String INTERNAL_AUDIT_PARAM_PREFIX = "audit.";
        
        protected AuditParameters auditParameters;
        protected MultiValueMap<String,String> paramMap;
        protected ProxiedUserDetails userDetails;
        
        private Request() {}
        
        /**
         * Constructs an audit request and delegates all validation of the request to {@link AuditParameters}
         * 
         * @param b
         *            {@link Builder} for the audit request
         * @throws IllegalArgumentException
         *             if validation fails on the resulting to {@link AuditParameters} instance
         */
        protected Request(Builder b) throws IllegalArgumentException {
            
            final MultiValueMap<String,String> params = null == b.params ? new LinkedMultiValueMap<>() : new LinkedMultiValueMap<>(b.params);
            
            // Remove internal audit-related params, in case those were passed in
            params.entrySet().removeIf(entry -> entry.getKey().startsWith(INTERNAL_AUDIT_PARAM_PREFIX));
            
            if (null != b.queryExpression) {
                params.add(AuditParameters.QUERY_STRING, b.queryExpression);
            }
            if (null != b.proxiedUserDetails) {
                this.userDetails = b.proxiedUserDetails;
                final DatawaveUser dwUser = this.getUserDetails().getPrimaryUser();
                if (null != dwUser.getAuths()) {
                    params.add(AuditParameters.QUERY_AUTHORIZATIONS, dwUser.getAuths().toString());
                }
                if (null != dwUser.getDn()) {
                    params.set(AuditParameters.USER_DN, dwUser.getDn().toString());
                }
            }
            if (null != b.auditType) {
                params.set(AuditParameters.QUERY_AUDIT_TYPE, b.auditType.name());
            }
            if (null != b.marking) {
                params.set(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, b.marking.toColumnVisibilityString());
            }
            if (null != b.queryLogic) {
                params.set(AuditParameters.QUERY_LOGIC_CLASS, b.queryLogic);
            }
            this.auditParameters = new AuditParameters();
            this.auditParameters.validate(params);
            this.paramMap = params;
        }
        
        public AuditParameters getAuditParameters() {
            return auditParameters;
        }
        
        public Auditor.AuditType getAuditType() {
            return auditParameters.getAuditType();
        }
        
        public ProxiedUserDetails getUserDetails() {
            return userDetails;
        }
        
        protected MultiValueMap<String,String> getAuditParametersAsMap() {
            return paramMap;
        }
        
        /**
         * Builder for base audit requests
         */
        public static class Builder {
            
            protected String queryExpression;
            protected String queryLogic;
            protected Auditor.AuditType auditType;
            protected MultiValueMap<String,String> params;
            protected SecurityMarking marking;
            protected ProxiedUserDetails proxiedUserDetails;
            
            public Builder withQueryExpression(String query) {
                this.queryExpression = query;
                return this;
            }
            
            public Builder withQueryLogic(String logicClass) {
                this.queryLogic = logicClass;
                return this;
            }
            
            public Builder withAuditType(Auditor.AuditType auditType) {
                this.auditType = auditType;
                return this;
            }
            
            public Builder withParams(MultiValueMap<String,String> params) {
                this.params = params;
                return this;
            }
            
            public Builder withProxiedUserDetails(ProxiedUserDetails proxiedUserDetails) {
                this.proxiedUserDetails = proxiedUserDetails;
                return this;
            }
            
            public Builder withMarking(SecurityMarking marking) {
                this.marking = marking;
                return this;
            }
            
            public Request build() {
                return new Request(this);
            }
        }
    }
}
