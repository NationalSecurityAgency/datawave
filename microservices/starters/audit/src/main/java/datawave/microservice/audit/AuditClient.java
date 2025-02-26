package datawave.microservice.audit;

import java.util.function.Supplier;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

import datawave.marking.SecurityMarking;
import datawave.microservice.authorization.jwt.JWTRestTemplate;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.audit.Auditor.AuditType;

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
    
    private Supplier<AuditParameters> validationSupplier;
    
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
        
        if (AuditType.NONE.equals(request.auditType)) {
            if (serviceProvider.getProperties().isSuppressAuditTypeNone()) {
                log.debug("Audit request with AuditType == {} was suppressed", AuditType.NONE);
                return;
            }
        }
        
        if (serviceProvider.getProperties().isFailFastAudit()) {
            Preconditions.checkNotNull(validationSupplier, "failFast validation enabled, but validationSupplier is null");
            validate(request, validationSupplier.get());
        }
        
        log.debug("Submitting audit request: {}", request);
        
        //@formatter:off
        ServiceInstance auditService = serviceProvider.getServiceInstance();
        UriComponents uri = UriComponentsBuilder.fromUri(auditService.getUri())
            .path(auditService.getServiceId() + requestPath)
            .build();

        log.debug("Submitting audit request to {}", uri);

        ResponseEntity<String> response = jwtRestTemplate.exchange(
            jwtRestTemplate.createRequestEntity(
                request.userDetails,
                request.paramMap,
                null,
                HttpMethod.POST, uri),
            String.class
        );

        if (response.getStatusCode().value() != HttpStatus.OK.value()) {
            String errorMessage = String.format("Audit request failed. Http Status: (%s, %s)",
                    response.getStatusCodeValue(),
                    response.getStatusCode().getReasonPhrase());
            log.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
        //@formatter:on
    }
    
    @Autowired
    @Qualifier("auditRequestValidator")
    public void setValidationSupplier(Supplier<AuditParameters> validationSupplier) {
        this.validationSupplier = validationSupplier;
    }
    
    public static AuditParameters validate(Request request, AuditParameters validator) {
        Preconditions.checkNotNull(request, "request cannot be null");
        Preconditions.checkNotNull(validator, "validator cannot be null");
        validator.clear();
        validator.validate(request.paramMap);
        return validator;
    }
    
    /**
     * Audit request for a given query
     *
     * @see Request.Builder
     */
    public static class Request {
        
        static final String INTERNAL_AUDIT_PARAM_PREFIX = "audit.";
        
        protected MultiValueMap<String,String> paramMap;
        protected DatawaveUserDetails userDetails;
        protected AuditType auditType;
        
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
                params.set(AuditParameters.QUERY_STRING, b.queryExpression);
            }
            if (null != b.datawaveUserDetails) {
                this.userDetails = b.datawaveUserDetails;
                final DatawaveUser dwUser = this.userDetails.getPrimaryUser();
                if (null != dwUser.getAuths() && !params.containsKey(AuditParameters.QUERY_AUTHORIZATIONS)) {
                    params.set(AuditParameters.QUERY_AUTHORIZATIONS, String.join(", ", dwUser.getAuths()));
                }
                if (null != dwUser.getDn()) {
                    params.set(AuditParameters.USER_DN, dwUser.getDn().toString());
                }
            }
            if (null != b.auditType) {
                this.auditType = b.auditType;
                params.set(AuditParameters.QUERY_AUDIT_TYPE, b.auditType.name());
            }
            if (null != b.marking) {
                params.set(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, b.marking.toColumnVisibilityString());
            }
            if (null != b.queryLogic) {
                params.set(AuditParameters.QUERY_LOGIC_CLASS, b.queryLogic);
            }
            
            this.paramMap = params;
        }
        
        public AuditType getAuditType() {
            return this.auditType;
        }
        
        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this).toString();
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
            protected DatawaveUserDetails datawaveUserDetails;
            
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
            
            public Builder withDatawaveUserDetails(DatawaveUserDetails datawaveUserDetails) {
                this.datawaveUserDetails = datawaveUserDetails;
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
