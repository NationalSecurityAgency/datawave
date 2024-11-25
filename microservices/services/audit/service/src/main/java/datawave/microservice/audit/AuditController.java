package datawave.microservice.audit;

import static datawave.webservice.common.audit.AuditParameters.AUDIT_ID;
import static datawave.webservice.common.audit.AuditParameters.QUERY_AUDIT_TYPE;
import static datawave.webservice.common.audit.AuditParameters.QUERY_AUTHORIZATIONS;
import static datawave.webservice.common.audit.AuditParameters.QUERY_DATE;
import static datawave.webservice.common.audit.AuditParameters.QUERY_LOGIC_CLASS;
import static datawave.webservice.common.audit.AuditParameters.QUERY_SECURITY_MARKING_COLVIZ;
import static datawave.webservice.common.audit.AuditParameters.QUERY_SELECTORS;
import static datawave.webservice.common.audit.AuditParameters.QUERY_STRING;
import static datawave.webservice.common.audit.AuditParameters.USER_DN;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.security.access.annotation.Secured;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import datawave.microservice.audit.common.AuditMessage;
import datawave.microservice.audit.common.AuditMessageSupplier;
import datawave.microservice.audit.config.AuditProperties;
import datawave.microservice.audit.config.AuditProperties.Retry;
import datawave.microservice.audit.health.HealthChecker;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * The AuditController presents the REST endpoints for the audit service.
 * <p>
 * Before returning success to the caller, the audit controller will verify that the audit message was successfully passed to our messaging infrastructure.
 * Also, if configured, the audit controller will verify that the message passing infrastructure is healthy before returning successfully to the user. If the
 * message passing infrastructure is unhealthy, or if we can't verify that the message was successfully passed to our messaging infrastructure, a 500 Internal
 * Server Error will be returned to the caller.
 */
@Tag(name = "Audit Controller /v1", description = "DataWave Query Auditing",
                externalDocs = @ExternalDocumentation(description = "Audit Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-audit-service"))
@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class AuditController {
    // Note: This must match 'confirmAckChannel' in the service configuration. Default set in bootstrap.yml.
    public static final String CONFIRM_ACK_CHANNEL = "confirmAckChannel";
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final AuditProperties auditProperties;
    
    private final AuditParameters restAuditParams;
    
    private final AuditMessageSupplier auditSource;
    
    @Autowired(required = false)
    private HealthChecker healthChecker;
    
    @Autowired(required = false)
    @Qualifier("fileAuditor")
    private Auditor fileAuditor;
    
    private static final Map<String,CountDownLatch> correlationLatchMap = new ConcurrentHashMap<>();
    
    public AuditController(AuditProperties auditProperties, @Qualifier("restAuditParams") AuditParameters restAuditParams, AuditMessageSupplier auditSource) {
        this.auditProperties = auditProperties;
        this.restAuditParams = restAuditParams;
        this.auditSource = auditSource;
    }
    
    /**
     * Receives producer confirm acks, and disengages the latch associated with the given correlation ID.
     * 
     * @param message
     *            the confirmation ack message
     */
    @ConditionalOnProperty(value = "audit.confirmAckEnabled", havingValue = "true", matchIfMissing = true)
    @ServiceActivator(inputChannel = CONFIRM_ACK_CHANNEL)
    public void processConfirmAck(Message<?> message) {
        Object headerObj = message.getHeaders().get(IntegrationMessageHeaderAccessor.CORRELATION_ID);
        
        if (headerObj != null) {
            String correlationId = headerObj.toString();
            if (correlationLatchMap.containsKey(correlationId)) {
                correlationLatchMap.get(correlationId).countDown();
            } else {
                log.warn("Unable to decrement latch for audit ID [{}]", correlationId);
            }
        } else {
            log.warn("No correlation ID found in confirm ack message");
        }
    }
    
    /**
     * Passes audit messages to the messaging infrastructure.
     * <p>
     * The audit ID is used as a correlation ID in order to ensure that a producer confirm ack is received. If a producer confirm ack is not received within the
     * specified amount of time, a 500 Internal Server Error will be returned to the caller.
     *
     * @param parameters
     *            The audit parameters to be sent
     */
    private boolean sendMessage(AuditParameters parameters) {
        if (healthChecker == null || healthChecker.isHealthy()) {
            String auditId = parameters.getAuditId();
            
            CountDownLatch latch = null;
            if (auditProperties.isConfirmAckEnabled()) {
                latch = new CountDownLatch(1);
                correlationLatchMap.put(auditId, latch);
            }
            
            boolean success = auditSource.send(MessageBuilder.withPayload(AuditMessage.fromParams(parameters)).setCorrelationId(auditId).build());
            
            if (auditProperties.isConfirmAckEnabled()) {
                try {
                    success = success && latch.await(auditProperties.getConfirmAckTimeoutMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    success = false;
                } finally {
                    correlationLatchMap.remove(auditId);
                }
            }
            
            return success;
        }
        
        return false;
    }
    
    /**
     * Performs auditing for the given parameters, via the configured Auditors.
     *
     * @param parameters
     *            the audit parameters
     * @return an audit ID, which can be used for tracking purposes
     */
    // @formatter:off
    @Operation(
            summary = "Submit an audit message with the given parameters.",
            description = "Audit messages will be forwarded to the configured auditors.")
    @ApiResponse(
            description = "if successful, returns the audit ID",
            responseCode = "200",
            content = @Content(schema = @Schema(implementation = String.class))
    )
    @Parameters({
            @Parameter(
                    name = USER_DN,
                    in = ParameterIn.QUERY,
                    description = "The user's DN",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "cn=test a. user, ou=example developers, o=example corp, c=us<cn=example corp ca, o=example corp, c=us>"),
            @Parameter(
                    name = QUERY_STRING,
                    in = ParameterIn.QUERY,
                    description = "The user-specified query",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "GENRES:[Action to Western]"),
            @Parameter(
                    name = QUERY_SELECTORS,
                    in = ParameterIn.QUERY,
                    description = "The selectors present in the query",
                    schema = @Schema(implementation = String.class),
                    example = "A,E,I,O,U"),
            @Parameter(
                    name = QUERY_AUTHORIZATIONS,
                    in = ParameterIn.QUERY,
                    description = "The user-selected authorizations",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC,PRIVATE,BAR,FOO"),
            @Parameter(
                    name = QUERY_AUDIT_TYPE,
                    in = ParameterIn.QUERY,
                    description = "The audit type",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "ACTIVE"),
            @Parameter(
                    name = QUERY_SECURITY_MARKING_COLVIZ,
                    in = ParameterIn.QUERY,
                    description = "The visibility to use when storing the audit record",
                    required = true,
                    schema = @Schema(implementation = String.class),
                    example = "PUBLIC"),
            @Parameter(
                    name = QUERY_DATE,
                    in = ParameterIn.QUERY,
                    description = "The date the user ran the query",
                    schema = @Schema(implementation = Long.class),
                    example = "1655410818951"),
            @Parameter(
                    name = QUERY_LOGIC_CLASS,
                    in = ParameterIn.QUERY,
                    description = "The query logic used",
                    schema = @Schema(implementation = String.class),
                    example = "EventQuery"),
            @Parameter(
                    name = AUDIT_ID,
                    in = ParameterIn.QUERY,
                    description = "An optional audit ID to use for tracking purposes",
                    schema = @Schema(implementation = String.class),
                    example = "my-audit-id")})
    // @formatter:on
    @Secured({"AuthorizedUser", "AuthorizedServer", "InternalUser", "Administrator"})
    @RequestMapping(path = "/audit", method = RequestMethod.POST)
    public String audit(@Parameter(hidden = true) @RequestParam MultiValueMap<String,String> parameters) {
        restAuditParams.clear();
        restAuditParams.validate(parameters);
        
        log.info("[{}] Received audit request with parameters {}", restAuditParams.getAuditId(), restAuditParams);
        
        if (!audit(restAuditParams))
            throw new RuntimeException("Unable to process audit message with id [" + restAuditParams.getAuditId() + "]");
        
        return restAuditParams.getAuditId();
    }
    
    public boolean audit(AuditParameters auditParameters) {
        
        boolean success;
        final long auditStartTime = System.currentTimeMillis();
        long currentTime;
        int attempts = 0;
        
        Retry retry = auditProperties.getRetry();
        
        do {
            if (attempts++ > 0) {
                try {
                    Thread.sleep(retry.getBackoffIntervalMillis());
                } catch (InterruptedException e) {
                    // Ignore -- we'll just end up retrying a little too fast
                }
            }
            
            if (log.isDebugEnabled())
                log.debug("[{}] Audit attempt {} of {}", auditParameters.getAuditId(), attempts, retry.getMaxAttempts());
            
            success = sendMessage(auditParameters);
            currentTime = System.currentTimeMillis();
        } while (!success && (currentTime - auditStartTime) < retry.getFailTimeoutMillis() && attempts < retry.getMaxAttempts());
        
        // last ditch effort to write the audit message to fileSystem for subsequent processing
        if (!success && fileAuditor != null) {
            success = true;
            try {
                log.debug("[{}] Attempting to log audit to the filesystem", auditParameters.getAuditId());
                
                fileAuditor.audit(auditParameters);
            } catch (Exception e) {
                log.error("[{}] Unable to save audit to the filesystem", auditParameters.getAuditId(), e);
                success = false;
            }
        }
        
        if (!success)
            log.warn("[{}] Audit failed. {attempts = {}, elapsedMillis = {}{}}", auditParameters.getAuditId(), attempts, (currentTime - auditStartTime),
                            ((fileAuditor != null) ? ", hdfsElapsedMillis = " + (System.currentTimeMillis() - currentTime) : ""));
        else
            log.info("[{}] Audit successful. {attempts = {}, elapsedMillis = {}{}}", auditParameters.getAuditId(), attempts, (currentTime - auditStartTime),
                            ((fileAuditor != null) ? ", hdfsElapsedMillis = " + (System.currentTimeMillis() - currentTime) : ""));
        
        return success;
    }
}
