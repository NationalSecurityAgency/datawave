package datawave.microservice.annotation.service;

import static datawave.annotation.util.v1.AnnotationUtils.injectAnnotationSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;
import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer;
import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.data.v1.AnnotationReader;
import datawave.annotation.data.v1.FederatedAnnotationReader;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.Validator;
import datawave.annotation.util.v1.AnnotationJsonUtils;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.annotation.util.v1.AnnotationValidators;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.runner.AccumuloConnectionRequestMap;
import datawave.microservice.annotation.common.AnnotationSupplier;
import datawave.microservice.annotation.service.config.AnnotationProperties;
import datawave.microservice.annotation.util.Metadata;
import datawave.microservice.annotation.util.lookup.service.LookupService;
import datawave.microservice.annotation.writers.AnnotationWriter;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.webservice.query.exception.QueryException;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;

/** Provides REST endpoints for reading and writing annotations to Datawave */
@Tag(name = "Annotation Controller /v1", description = "Operations related to annotations")
@Slf4j
@RestController
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator", "AnnotationWriter"})
@RequestMapping(path = "/v1", produces = {MediaType.APPLICATION_JSON_VALUE})
public class AnnotationControllerV1 {
    // Note: This must match 'annotationAckChannel' in the service configuration. Default set in bootstrap.yml.
    public static final String ANNOTATION_ACK_CHANNEL = "annotationAckChannel";

    public static final String ANNOTATION_SERVICE_SYSTEM_FROM = "annotation";

    private final AnnotationProperties annotationProperties;

    /** used for reading from accumulo */
    private final AccumuloConnectionFactory connectionFactory;

    /** tracks accumulo connections */
    private final AccumuloConnectionRequestMap accumuloConnectionRequestMap = new AccumuloConnectionRequestMap();

    /** used to transform external identifiers into internal identifiers */
    private final LookupService lookupService;

    /** executor service for federated annotation reads (fan-out across annotation + truthmark tables) */
    private final ExecutorService federatedReadExecutorService;

    // Configuration for the data access object
    private final TimestampTransformer timestampTransformer;
    private final VisibilityTransformer visibilityTransformer;

    private static final Map<String,CountDownLatch> correlationLatchMap = new ConcurrentHashMap<>();

    /** used as a 'sink' for annotation writes */
    private final AnnotationSupplier annotationSource;

    /** used as a backup writer for annotations */
    @Autowired(required = false)
    @Qualifier("fileAnnotationWriter")
    private AnnotationWriter fileAnnotationWriter;

    @Autowired
    public AnnotationControllerV1(AccumuloConnectionFactory factory, LookupService lookupService, AnnotationProperties annotationProperties,
                    TimestampTransformer timestampTransformer, VisibilityTransformer visibilityTransformer, AnnotationSupplier annotationSource,
                    ExecutorService federatedReadExecutorService) {
        this.connectionFactory = factory;
        this.lookupService = lookupService;
        this.annotationProperties = annotationProperties;
        this.timestampTransformer = timestampTransformer;
        this.visibilityTransformer = visibilityTransformer;
        this.annotationSource = annotationSource;
        this.federatedReadExecutorService = federatedReadExecutorService;
    }

    @GetMapping("/source/{analyticHash}")
    public ResponseEntity<?> getAnnotationSource(@PathVariable String analyticHash, @RequestParam MultiValueMap<String,String> queryParameters,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {

        final RequestContext context = new RequestContext(queryParameters, currentUser, annotationProperties, connectionFactory, accumuloConnectionRequestMap,
                        visibilityTransformer, timestampTransformer, federatedReadExecutorService);

        try {
            final AnnotationReader annotationDataAccess = context.initializeAnnotationService();
            Optional<AnnotationSource> results = annotationDataAccess.getAnnotationSource(analyticHash);
            if (results.isEmpty()) {
                return jsonNotFound("No annotation source found for analyticHash: " + analyticHash);
            }
            return jsonOk(results.get());
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation source: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }

    }

    @GetMapping("/{idType}/{id}/types")
    public ResponseEntity<?> getAnnotationTypes(@Parameter(description = "The type of identifier to use for lookup") @PathVariable String idType,
                    @Parameter(description = "The identifier to use for lookup") @PathVariable String id,
                    @RequestParam MultiValueMap<String,String> queryParameters, @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        // TODO sanitize input to make sure it contains nothing weird like nulls.
        final RequestContext context = new RequestContext(queryParameters, currentUser, annotationProperties, connectionFactory, accumuloConnectionRequestMap,
                        visibilityTransformer, timestampTransformer, federatedReadExecutorService);

        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(idType, id, queryParameters, currentUser);
            if (metadata.isEmpty()) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }
            final AnnotationReader annotationDataAccess = context.initializeAnnotationService();
            final Map<Metadata,Collection<String>> results = new HashMap<>();
            for (Metadata md : metadata) {
                final Collection<String> types = annotationDataAccess.getAnnotationTypes(md.getRow(), md.getDataType(), md.getInternalId());
                if (!types.isEmpty()) {
                    results.put(md, types);
                }
            }
            if (results.isEmpty()) {
                return jsonNotFound("annotation types", idType, id, metadata.toString(), null, null, null);
            }
            return jsonOk(results);
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }
    }

    @GetMapping("/{idType}/{id}")
    public ResponseEntity<?> getAnnotationsFor(@PathVariable String idType, @PathVariable String id, @RequestParam MultiValueMap<String,String> queryParameters,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        final RequestContext context = new RequestContext(queryParameters, currentUser, annotationProperties, connectionFactory, accumuloConnectionRequestMap,
                        visibilityTransformer, timestampTransformer, federatedReadExecutorService);

        // TODO sanitize input to make sure it contains nothing weird like nulls.
        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(idType, id, queryParameters, currentUser);
            if (metadata.isEmpty()) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }
            final AnnotationReader annotationDataAccess = context.initializeAnnotationService();

            final List<Annotation> results = new ArrayList<>();
            for (Metadata md : metadata) {
                final Collection<Annotation> annotations = annotationDataAccess.getAnnotations(md.getRow(), md.getDataType(), md.getInternalId());
                if (!annotations.isEmpty()) {
                    List<Annotation> annotationsWithSources = lookupAndInjectAnnotationSources(context, annotations);
                    results.addAll(annotationsWithSources);
                }
            }
            if (results.isEmpty()) {
                return jsonNotFound("annotations", idType, id, metadata.toString(), null, null, null);
            }
            return jsonOk(results);
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }
    }

    @GetMapping("/{idType}/{id}/type/{annotationType}")
    public ResponseEntity<?> getAnnotationsByType(@PathVariable String idType, @PathVariable String id, @PathVariable String annotationType,
                    @RequestParam MultiValueMap<String,String> queryParameters, @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        final RequestContext context = new RequestContext(queryParameters, currentUser, annotationProperties, connectionFactory, accumuloConnectionRequestMap,
                        visibilityTransformer, timestampTransformer, federatedReadExecutorService);

        // TODO sanitize input to make sure it contains nothing weird like nulls.
        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(idType, id, queryParameters, currentUser);
            if (metadata.isEmpty()) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }
            final AnnotationReader annotationDataAccess = context.initializeAnnotationService();

            final List<Annotation> results = new ArrayList<>();
            for (Metadata md : metadata) {
                final Collection<Annotation> annotations = annotationDataAccess.getAnnotationsForType(md.getRow(), md.getDataType(), md.getInternalId(),
                                annotationType);
                if (!annotations.isEmpty()) {
                    List<Annotation> annotationsWithSources = lookupAndInjectAnnotationSources(context, annotations);
                    results.addAll(annotationsWithSources);
                }
            }
            if (results.isEmpty()) {
                return jsonNotFound("annotations of type", idType, id, metadata.toString(), annotationType, null, null);
            }
            return jsonOk(results);
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }
    }

    @GetMapping("/{idType}/{id}/annotation/{annotationId}")
    public ResponseEntity<?> getAnnotation(@PathVariable String idType, @PathVariable String id, @PathVariable String annotationId,
                    @RequestParam MultiValueMap<String,String> queryParameters, @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        final RequestContext context = new RequestContext(queryParameters, currentUser, annotationProperties, connectionFactory, accumuloConnectionRequestMap,
                        visibilityTransformer, timestampTransformer, federatedReadExecutorService);

        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(idType, id, queryParameters, currentUser);
            if (metadata.isEmpty()) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }
            final AnnotationReader annotationDataAccess = context.initializeAnnotationService();

            final List<Annotation> results = new ArrayList<>();
            for (Metadata md : metadata) {
                final Optional<Annotation> annotations = annotationDataAccess.getAnnotation(md.getRow(), md.getDataType(), md.getInternalId(), annotationId);
                if (annotations.isPresent()) {
                    Annotation annotationWithSource = lookupAndInjectAnnotationSource(context, annotations.get());
                    results.add(annotationWithSource);
                }
            }
            if (results.isEmpty()) {
                return jsonNotFound("annotations", idType, id, metadata.toString(), null, annotationId, null);
            }
            return jsonOk(results);
        } catch (Exception e) {
            final String message = String.format("Internal error fetching annotation: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }
    }

    @PostMapping(path = "/{idType}/{id}/annotation", consumes = MediaType.APPLICATION_JSON_VALUE)
    @RolesAllowed({"AnnotationWriter"})
    public ResponseEntity<?> addAnnotation(@PathVariable String idType, @PathVariable String id, @RequestBody String body,
                    @RequestParam MultiValueMap<String,String> queryParameters, @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        final RequestContext context = new RequestContext(queryParameters, currentUser, annotationProperties, connectionFactory, accumuloConnectionRequestMap,
                        visibilityTransformer, timestampTransformer, federatedReadExecutorService);

        try {
            final Annotation rawAnnotation = AnnotationJsonUtils.annotationFromJson(body);
            final Validator.ValidationState<Annotation> validationState = AnnotationValidators.checkAnnotation(rawAnnotation);
            if (!validationState.isValid()) {
                final String message = String.format("Invalid annotation json: %s", validationState.getErrors());
                log.error(message);
                return jsonError(message);
            }

            final List<Metadata> metadataList = lookupDocumentIdentifier(idType, id, queryParameters, currentUser);
            if (metadataList.isEmpty()) {
                final String message = String.format("No internal identifier found for '%s:%s'", idType, id);
                log.info(message);
                return jsonNotFound(message);
            } else if (metadataList.size() > 1) {
                final String message = String.format("Multiple internal identifiers found for '%s:%s' must choose an id with a single internal id: %s", idType,
                                id, metadataList);
                log.error(message);
                return jsonError(message);
            }

            final Metadata metadata = metadataList.get(0);

            //@formatter:off
            final Annotation localizedAnnotation = rawAnnotation.toBuilder()
                    .setShard(metadata.getRow())
                    .setDataType(metadata.getDataType())
                    .setUid(metadata.getInternalId())
                    .build();
            //@formatter:on

            Optional<Annotation> addResult = writeAnnotation(localizedAnnotation);
            if (addResult.isPresent()) {
                log.debug("Successfully added annotation: {}", addResult.get());
                return jsonOk(addResult.get());
            }

            // if we make it here, there was a problem
            String message = String.format(
                            "Internal error: Optional return from dao addAnnotation was empty, id: %s, idType %s, internal id %s, localized annotation: %s",
                            idType, id, metadata, localizedAnnotation);
            log.error(message);
            return jsonError(message);
        } catch (InvalidProtocolBufferException e) {
            final String message = String.format("Invalid annotation json: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        } catch (QueryException e) {
            final String message = String.format("Internal error adding annotation: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }
    }

    @PutMapping(path = "/{idType}/{id}/annotation/{annotationId}", consumes = MediaType.APPLICATION_JSON_VALUE)
    @RolesAllowed({"AnnotationWriter"})
    public ResponseEntity<?> updateAnnotation(@PathVariable String idType, @PathVariable String id, @PathVariable String annotationId, @RequestBody String body,
                    @RequestParam MultiValueMap<String,String> queryParameters, @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        final RequestContext context = new RequestContext(queryParameters, currentUser, annotationProperties, connectionFactory, accumuloConnectionRequestMap,
                        visibilityTransformer, timestampTransformer, federatedReadExecutorService);

        try {
            final Annotation rawAnnotation = AnnotationJsonUtils.annotationFromJson(body);
            final Validator.ValidationState<Annotation> validationState = AnnotationValidators.checkAnnotationUpdate(rawAnnotation);
            if (!validationState.isValid()) {
                final String message = String.format("Invalid annotation json: %s", validationState.getErrors());
                log.error(message);
                return jsonError(message);
            }

            final List<Metadata> metadataList = lookupDocumentIdentifier(idType, id, queryParameters, currentUser);
            if (metadataList.isEmpty()) {
                final String message = String.format("No internal identifier found for '%s:%s'", idType, id);
                log.info(message);
                return jsonNotFound(message);
            } else if (metadataList.size() > 1) {
                final String message = String.format("Multiple internal identifiers found for '%s:%s' must choose an id with a single internal id: %s", idType,
                                id, metadataList);
                log.error(message);
                return jsonError(message);
            }

            final Metadata metadata = metadataList.get(0);

            final AnnotationReader annotationDataAccess = context.initializeAnnotationService();
            final Optional<Annotation> targetAnnotation = annotationDataAccess.getAnnotation(metadata.getRow(), metadata.getDataType(),
                            metadata.getInternalId(), annotationId);
            if (targetAnnotation.isEmpty()) {
                return jsonNotFound("annotations", idType, id, metadata.toString(), null, annotationId, null);
            }

            //@formatter:off
            final Annotation localizedAnnotation = rawAnnotation.toBuilder()
                    .setShard(metadata.getRow())
                    .setDataType(metadata.getDataType())
                    .setUid(metadata.getInternalId())
                    .build();
            //@formatter:on

            // link the replacement annotation back to the annotation it is updating so the two remain associated in the store.
            final Annotation referencedAnnotation = AnnotationUtils.injectUpdateReference(localizedAnnotation, annotationId);

            Optional<Annotation> addResult = writeAnnotation(referencedAnnotation);
            if (addResult.isPresent()) {
                log.debug("Successfully updated annotation: {}", addResult.get());
                return jsonOk(addResult.get());
            }
            // if we make it here, there was a problem
            String message = String.format(
                            "Internal error: Optional return from dao updateAnnotation was empty, id: %s, idType %s, internal id %s, localized annotation: %s",
                            idType, id, metadata, referencedAnnotation);
            log.error(message);
            return jsonError(message);
        } catch (InvalidProtocolBufferException e) {
            final String message = String.format("Invalid annotation json: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        } catch (QueryException e) {
            final String message = String.format("Internal error updating annotation: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }
    }

    @GetMapping("/{idType}/{id}/annotation/{annotationId}/segment/{segmentHash}")
    public ResponseEntity<?> getAnnotationSegment(@PathVariable String idType, @PathVariable String id, @PathVariable String annotationId,
                    @PathVariable String segmentHash, @RequestParam MultiValueMap<String,String> queryParameters,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        final RequestContext context = new RequestContext(queryParameters, currentUser, annotationProperties, connectionFactory, accumuloConnectionRequestMap,
                        visibilityTransformer, timestampTransformer, federatedReadExecutorService);

        try {
            final List<Metadata> metadata = lookupDocumentIdentifier(idType, id, queryParameters, currentUser);
            if (metadata.isEmpty()) {
                return jsonNotFound(String.format("No internal identifier found for '%s:%s'", idType, id));
            }
            final AnnotationReader annotationDataAccess = context.initializeAnnotationService();

            final Map<Metadata,Annotation> annotationResults = new HashMap<>();
            for (Metadata md : metadata) {
                final Optional<Annotation> annotation = annotationDataAccess.getAnnotation(md.getRow(), md.getDataType(), md.getInternalId(), annotationId);
                annotation.ifPresent(value -> annotationResults.put(md, value));
            }

            if (annotationResults.isEmpty()) {
                return jsonNotFound("annotations", idType, id, metadata.toString(), null, annotationId, segmentHash);
            }

            final Map<Metadata,Collection<Segment>> results = new HashMap<>();
            for (Map.Entry<Metadata,Annotation> entry : annotationResults.entrySet()) {
                // now select only the segments that were requested.
                List<Segment> matchingSegments = new ArrayList<>();
                for (Segment s : entry.getValue().getSegmentsList()) {
                    if (s.getSegmentHash().equals(segmentHash)) {
                        matchingSegments.add(s);
                    }
                }
                if (!matchingSegments.isEmpty()) {
                    results.put(entry.getKey(), matchingSegments);
                }
            }

            if (results.isEmpty()) {
                return jsonNotFound("segments", idType, id, metadata.toString(), null, annotationId, segmentHash);
            }
            return jsonOk(results);
        } catch (QueryException e) {
            final String message = String.format("Internal error fetching segment: %s", e.getMessage());
            log.error(message, e);
            return jsonError(message);
        } finally {
            context.returnAccumuloClient();
        }
    }

    /* package-private for unit testing */
    Optional<Annotation> writeAnnotation(Annotation annotation) {
        Annotation identifiedAnnotation = AnnotationUtils.injectAllHashes(annotation);
        Optional<Annotation> result;

        final long writeStartTime = System.currentTimeMillis();
        long currentTime;
        int attempts = 0;

        AnnotationProperties.Retry retry = annotationProperties.getRetry();

        do {
            if (attempts++ > 0) {
                try {
                    // noinspection BusyWait
                    Thread.sleep(retry.getBackoffIntervalMillis());
                } catch (InterruptedException e) {
                    // Ignore -- we'll just end up retrying a little too fast
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("[{}] Annotation write attempt {} of {}", identifiedAnnotation.getAnnotationId(), attempts, retry.getMaxAttempts());
            }

            result = sendAnnotation(identifiedAnnotation);
            currentTime = System.currentTimeMillis();
        } while (result.isEmpty() && retry.noTimeout(writeStartTime, currentTime) && retry.hasAttemptsRemaining(attempts));

        if (result.isEmpty() && fileAnnotationWriter != null) {
            result = Optional.of(identifiedAnnotation);
            try {
                log.debug("[{}] Attempting to write annotation to the filesystem", identifiedAnnotation.getAnnotationId());
                fileAnnotationWriter.write(identifiedAnnotation);
            } catch (Exception e) {
                log.error("[{}] Unable to save annotation to the filesystem", identifiedAnnotation.getAnnotationId(), e);
                result = Optional.empty();
            }
        }

        if (result.isEmpty()) {
            log.warn("[{}] Annotation write failed. {attempts = {}, elapsedMillis = {}{}}", identifiedAnnotation.getAnnotationId(), attempts,
                            (currentTime - writeStartTime),
                            ((fileAnnotationWriter != null) ? ", hdfsElapsedMillis = " + (System.currentTimeMillis() - currentTime) : ""));
        } else {
            log.info("[{}] Annotation write successful. {attempts = {}, elapsedMillis = {}{}}", identifiedAnnotation.getAnnotationId(), attempts,
                            (currentTime - writeStartTime),
                            ((fileAnnotationWriter != null) ? ", hdfsElapsedMillis = " + (System.currentTimeMillis() - currentTime) : ""));
        }

        return result;
    }

    /**
     * Adapter between writeAnnotation(Annotation) and sendAnnotationMessage that manages the fact that messages sent and received by this controller only send
     * a single Annotation at a time
     *
     * @param annotation
     *            the annotation to send
     * @return an Optional containing the annotation that was sent, possibly empty
     */
    /* package-private for unit testing */
    Optional<Annotation> sendAnnotation(Annotation annotation) {
        //@formatter:off
        AnnotationMessage annotationMessage = AnnotationMessage.newBuilder()
                .addAnnotations(annotation)
                .setSource(annotationProperties.getSystemFrom())
                .build();
        //@formatter:on

        Optional<AnnotationMessage> result = sendAnnotationMessage(annotationMessage);

        if (result.isEmpty()) {
            return Optional.empty();
        }

        AnnotationMessage resultMessage = result.get();
        List<Annotation> annotationList = resultMessage.getAnnotationsList();
        if (annotationList.isEmpty()) {
            return Optional.empty();
        } else if (annotationList.size() > 1) {
            log.warn("Unexpected annotation list size in AnnotationMessage id {}: {}", resultMessage.getAnnotationMessageId(), annotationList.size());
        }
        return Optional.of(annotationList.get(0));
    }

    /* package-private for unit testing */
    Optional<AnnotationMessage> sendAnnotationMessage(AnnotationMessage annotationMessage) {
        AnnotationMessage identifiedMessage = annotationMessage.getAnnotationMessageId().isBlank()
                        ? AnnotationUtils.injectAnnotationMessageHash(annotationMessage)
                        : annotationMessage;
        String annotationMessageId = identifiedMessage.getAnnotationMessageId();

        boolean success;
        if (annotationProperties.isAnnotationAckEnabled()) {
            final CountDownLatch newLatch = new CountDownLatch(1);
            // if a send for this exact annotationMessageId is already in flight, reuse its latch instead of overwriting it -- overwriting
            // would orphan the earlier caller's latch so that it never gets counted down and always times out.
            final CountDownLatch existingLatch = correlationLatchMap.putIfAbsent(annotationMessageId, newLatch);
            final boolean isOriginalSender = existingLatch == null;
            final CountDownLatch latch = isOriginalSender ? newLatch : existingLatch;

            success = !isOriginalSender || annotationSource.send(MessageBuilder.withPayload(identifiedMessage).setCorrelationId(annotationMessageId).build());

            try {
                success = success && latch.await(annotationProperties.getAnnotationAckTimeoutMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                success = false;
            } finally {
                // only the original sender owns this latch entry and should remove it; a duplicate caller must leave it alone
                // so the original sender's await(...) above can still be satisfied by the eventual ack.
                if (isOriginalSender) {
                    correlationLatchMap.remove(annotationMessageId, latch);
                }
            }
        } else {
            success = annotationSource.send(MessageBuilder.withPayload(identifiedMessage).setCorrelationId(annotationMessageId).build());
        }

        return success ? Optional.of(identifiedMessage) : Optional.empty();
    }

    /**
     * Receives producer confirm acknowledgements, and disengages the latch associated with the given correlation ID.
     *
     * @param message
     *            the confirmation acknowledgements message
     */
    @ConditionalOnProperty(value = "annotation.annotationAckEnabled", havingValue = "true", matchIfMissing = true)
    @ServiceActivator(inputChannel = ANNOTATION_ACK_CHANNEL)
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
     * Look up the internal id for the annotation and return a 3 part tuple of shard, datatype uid
     *
     * @param idType
     *            the type of id provided
     * @param id
     *            the id itself.
     * @param queryParameters
     *            the request query parameters used to prepare any remote lookup request
     * @param currentUser
     *            the current user whose authorizations should be applied to the lookup
     * @return a list of zero to many Metadata objects with the internal shard, datatype, uid and table name of the identifier(s) provided. The list will be
     *         empty if no identifier could be found using the authorizations and query logic employed by this class.
     * @throws QueryException
     *             if the id is malformed.
     */
    private List<Metadata> lookupDocumentIdentifier(String idType, String id, MultiValueMap<String,String> queryParameters, DatawaveUserDetails currentUser)
                    throws QueryException {
        // If the idType is RECORD_ID or DOCUMENT, treat the id provided as an internal id and perform a direct lookup
        // against the annotations table, if that's enabled.
        if (idType.equals("DOCUMENT") || idType.equals("RECORD_ID")) {
            if (!annotationProperties.isEnableInternalIdLookup()) {
                final String message = String.format("Internal identifier lookup is disabled for '%s:%s' please use a valid document id type.", idType, id);
                throw new QueryException(message);
            }

            return parseDocumentIdentifier(id);
        }

        return lookupService.executeLookupUUIDQuery(idType, id, prepareLookupParameters(queryParameters), annotationProperties.getSystemFrom(), currentUser);
    }

    private String prepareLookupParameters(MultiValueMap<String,String> queryParameters) {
        // TODO: implement this;
        return "";
    }

    /**
     * Given a list of annotations, retrieve the annotation source information that is referenced by their analyticHash. If an analyticHash is not found, we
     * simply return the annotation without the source data injected. Currently, no errors are logged.
     *
     * @param context
     *            the request-scoped context that provides cached annotation source lookups
     * @param annotations
     *            the annotations to inject sources into
     * @return return annotations with sources injected where possible.
     */
    /**
     * When returning the source in the context of an annotation, mask/remove certain metadata from the source (e.g., visibility) because the metadata on the
     * annotation itself takes precedence. Mirrors the legacy {@code AnnotationManagerBean.maskSourceMetadata(AnnotationSource)} behavior.
     *
     * @param annotationSource
     *            the annotation source with metadata fields to mask
     * @return a new source with masked fields removed, or the original source if nothing was masked
     */
    private AnnotationSource maskSourceMetadata(AnnotationSource annotationSource) {
        final List<String> fieldsToMask = annotationProperties.getMaskSourceMetadata();
        if (fieldsToMask == null || fieldsToMask.isEmpty()) {
            // no fields to mask, make no changes.
            return annotationSource;
        }

        AnnotationSource.Builder builder = null;
        for (String key : fieldsToMask) {
            if (annotationSource.containsMetadata(key)) {
                if (builder == null) {
                    builder = annotationSource.toBuilder();
                }
                builder.removeMetadata(key);
            }
        }

        return (builder == null) ? annotationSource : builder.build();
    }

    private List<Annotation> lookupAndInjectAnnotationSources(RequestContext context, Collection<Annotation> annotations) {
        final List<Annotation> results = new ArrayList<>();
        for (Annotation a : annotations) {
            results.add(lookupAndInjectAnnotationSource(context, a));
        }
        return results;
    }

    /**
     * Given an annotation, retrieve the annotation source information that is referenced by their analyticHash. Employs a per-request hash so we don't look up
     * a single source multiple times.
     *
     * @param context
     *            the request-scoped context that provides cached annotation source lookups
     * @param a
     *            the annotation whose source should be looked up and injected when available
     * @return the original annotation, or the same annotation with its source injected when a matching source is found
     */
    private Annotation lookupAndInjectAnnotationSource(RequestContext context, Annotation a) {
        // no need to inject a source if we already have one.
        if (a.hasSource()) {
            log.warn("Strange, this annotation already has a source. Annotation {}/{}/{} {}, using analyticHash {}", a.getShard(), a.getDataType(), a.getUid(),
                            a.getAnnotationId(), a.getAnalyticSourceHash());
            return a;
        }

        if (StringUtils.isBlank(a.getAnalyticSourceHash())) {
            log.warn("Strange, this annotation does not have an analytic hash. Annotation {}/{}/{} {}", a.getShard(), a.getDataType(), a.getUid(),
                            a.getAnnotationId());
            return a;
        }

        // do the deed and cache the results.
        final String analyticHash = a.getAnalyticSourceHash();
        final Optional<AnnotationSource> result = context.getAnnotationSource(analyticHash);
        if (result.isPresent()) {
            return injectAnnotationSource(a, maskSourceMetadata(result.get()));
        } else {
            log.debug("No analytic source found for annotation {}/{}/{} {}, using analyticHash {}", a.getShard(), a.getDataType(), a.getUid(),
                            a.getAnnotationId(), a.getAnalyticSourceHash());
            return a;
        }
    }

    /**
     * Parse an identifier that is expected to be in the shardId/datatype/eventUID format into a Metadata object
     *
     * @param identifier
     *            the identifier to parse
     * @return a singleton list the corresponding Metadata object
     * @throws IllegalArgumentException
     *             if the identifier is not in the expected shardId/datatype/eventUID format.
     */
    private List<Metadata> parseDocumentIdentifier(String identifier) {
        final String[] parts = identifier.split("[/:]");
        if (parts.length != 3) {
            throw new IllegalArgumentException(
                            "Identifier does not specify all needed 3 parts. Identifier must be in the form 'shardId/datatype/eventUID' or 'shardId:datatype:eventUID'.");
        }

        final Metadata md = new Metadata(annotationProperties.getShardTableName(), parts[0], parts[1], parts[2]);
        return Collections.singletonList(md);
    }

    private static ResponseEntity<?> jsonNotFound(String objectType, String idType, String id, String internalId, String annotationType, String annotationId,
                    String segmentHash) {
        String message = id.contains(internalId) ? String.format("No %s found for identifier: '%s:%s'", objectType, idType, id)
                        : String.format("No %s found for identifier '%s:%s', internalId: '%s'", objectType, idType, id, internalId);

        if (!StringUtils.isEmpty(annotationType)) {
            message += String.format(", annotationType '%s'", annotationType);
        }
        if (!StringUtils.isEmpty(annotationId)) {
            message += String.format(", annotationId '%s'", annotationId);
        }
        if (!StringUtils.isEmpty(segmentHash)) {
            message += String.format(", segmentHash '%s'", segmentHash);
        }

        return jsonNotFound(message);
    }

    private static ResponseEntity<String> jsonNotFound(String message) {
        String response = "{\"message\":\"" + message + "\"}";
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
    }

    private static ResponseEntity<String> jsonError(String message) {
        String response = "{\"message\":\"" + message + "\"}";
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }

    private static <T> ResponseEntity<T> jsonOk(T body) {
        return ResponseEntity.status(HttpStatus.OK).contentType(MediaType.APPLICATION_JSON).body(body);
    }

    /** Per-request initialization code and related state */
    protected static final class RequestContext {

        public static final String QUERY_AUTHORIZATIONS = "auths";

        private final AnnotationProperties config;

        private final AccumuloConnectionFactory connectionFactory;

        private final AccumuloConnectionRequestMap accumuloConnectionRequestBean;

        /** the current user running the query. */
        private final DatawaveUserDetails currentUser;

        /** the dn of the user performing this request */
        private final String userDn;

        /** proxy servers involved in this request */
        private final Collection<String> proxyServers;

        private final String queryAuthorizations;

        private final VisibilityTransformer visibilityTransformer;
        private final TimestampTransformer timestampTransformer;
        private final ExecutorService federatedReadExecutorService;

        /** the final set of merged query and user authorizations. */
        Set<Authorizations> authorizations;

        /** the accumulo client to use for this request - obtained from the connection pool and must be returned. */
        AccumuloClient client;

        /** used to _read_ annotations directly from accumulo, scoped to the caller's authorizations */
        AnnotationReader annotationDataAccess;

        /** Cache lookups for unique analytic source hashes so we don't perform lookups more than once. TODO: make this a proper cross-request cache? */
        private final Map<String,Optional<AnnotationSource>> retrievedSourcesCache = new HashMap<>();

        /**
         * Initialize the request context with the objects needed to perform various request state initialization. Validation of the objects provided is
         * performed in the various initialize methods exposed by this class. Each of the objects provided as parameters are expected to be shared across many
         * requests.
         *
         * @param queryParameters
         *            the incoming request query parameters, including any requested authorization overrides
         * @param currentUser
         *            the current user whose identity and authorizations are used for the request
         * @param config
         *            the annotation manager configuration
         * @param connectionFactory
         *            the accumulo connection factory - used for getting accumulo clients
         * @param accumuloConnectionRequestBean
         *            the accumulo connection request bean - used for tracking accumulo clients requests
         * @param visibilityTransformer
         *            visibility transformer implementation
         * @param timestampTransformer
         *            timestamp transformer implementation
         */
        protected RequestContext(MultiValueMap<String,String> queryParameters, DatawaveUserDetails currentUser, AnnotationProperties config,
                        AccumuloConnectionFactory connectionFactory, AccumuloConnectionRequestMap accumuloConnectionRequestBean,
                        VisibilityTransformer visibilityTransformer, TimestampTransformer timestampTransformer, ExecutorService federatedReadExecutorService) {

            this.currentUser = currentUser;

            this.config = config;
            this.connectionFactory = connectionFactory;
            this.accumuloConnectionRequestBean = accumuloConnectionRequestBean;
            this.visibilityTransformer = visibilityTransformer;
            this.timestampTransformer = timestampTransformer;
            this.federatedReadExecutorService = federatedReadExecutorService;

            this.userDn = currentUser.getName();
            this.proxyServers = currentUser.getProxyServers();

            // TODO: allow downgrading by reading query auths from query parameters.
            this.queryAuthorizations = queryParameters.getFirst(QUERY_AUTHORIZATIONS);
        }

        /**
         * Calculate the authorizations for this request based on the principal and the queryAuths if any.
         *
         * @return a valid set of query auths, will throw an exception if this isn't possible.
         * @throws QueryException
         *             and exception if there was a problem calculating the auths
         */
        private Set<Authorizations> initializeAuthorizations() throws QueryException {
            if (authorizations == null) {
                log.trace("Initializing authorizations: userDn: {}, query: {}", userDn, queryAuthorizations);
                if (currentUser == null) {
                    throw new QueryException("Failed to get user principal from request, unable to proceed");
                }

                try {
                    if (queryAuthorizations == null) {
                        authorizations = AuthorizationsUtil.buildAuthorizations(currentUser.getAuthorizations());
                    } else {
                        final String downgradedAuths = AuthorizationsUtil.downgradeUserAuths(queryAuthorizations, currentUser, currentUser);
                        authorizations = AuthorizationsUtil.buildAuthorizations(Collections.singleton(AuthorizationsUtil.splitAuths(downgradedAuths)));
                    }

                } catch (Exception e) {
                    throw new QueryException("Failed to get user query authorizations", e);
                }

                log.debug("Authorizations initialized: userDn: {}, query: {}, final auths: {}", userDn, queryAuthorizations, authorizations);
            }
            return authorizations;
        }

        /**
         * Initialize the accumulo client
         *
         * @return a valid client, will throw an exception if this isn't possibly
         * @throws QueryException
         *             if the client can't be initialized.
         */
        protected AccumuloClient initializeAccumuloClient() throws QueryException {
            if (client == null) {
                log.trace("Initializing accumulo client");
                UUID transactionUUID = java.util.UUID.randomUUID();

                if (connectionFactory == null) {
                    throw new QueryException("The accumulo connection factory isn't present, unable to proceed");
                }

                Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
                if (trackingMap != null) {
                    trackingMap.put("query.user", userDn);
                    trackingMap.put("query.id", transactionUUID.toString());
                    trackingMap.put("query.query", "annotation manager");
                } else {
                    log.info("Accumulo connection tracking map was null, this isn't fatal, but odd.");
                }

                if (accumuloConnectionRequestBean == null) {
                    throw new QueryException("The accumulo connection request manager isn't present, unable to proceed");
                }

                accumuloConnectionRequestBean.requestBegin(transactionUUID.toString(), userDn, trackingMap);
                try {
                    client = connectionFactory.getClient(userDn, proxyServers, config.getConnPoolName(), config.getPriority(), trackingMap);
                } catch (Exception e) {
                    throw new QueryException("Unable to get Accumulo client, exception encountered: ", e);
                } finally {
                    accumuloConnectionRequestBean.requestEnd(transactionUUID.toString());
                }
                log.debug("Accumulo client initialized successfully");

            }
            return client;
        }

        /**
         * Initialize the annotation service, specifically the data access layer.
         *
         * @return a valid annotation data access object, throws an exception if this isn't possible.
         * @throws QueryException
         *             if the annotation data access object can't be initialized.
         */
        protected AnnotationReader initializeAnnotationService() throws QueryException {
            if (annotationDataAccess == null) {
                log.trace("Initializing annotation data access layer");
                final Set<Authorizations> authorizations = initializeAuthorizations();
                final AccumuloClient client = initializeAccumuloClient();
                final AccumuloAnnotationSerializer annotationSerializer = new AccumuloAnnotationSerializer(visibilityTransformer, timestampTransformer);
                final AccumuloAnnotationSourceSerializer annotationSourceSerializer = new AccumuloAnnotationSourceSerializer(visibilityTransformer,
                                timestampTransformer);

                // Construct DAOs for both the annotation table pair and the truthmark table pair
                final AnnotationDataAccess annotationDao = new AnnotationDataAccess(client, authorizations, config.getAnnotationTableName(),
                                config.getAnnotationSourceTableName(), annotationSerializer, annotationSourceSerializer);
                final AnnotationDataAccess truthmarkDao = new AnnotationDataAccess(client, authorizations, config.getTruthmarkTableName(),
                                config.getTruthmarkSourceTableName(), annotationSerializer, annotationSourceSerializer);

                // Fan out reads across both table pairs (matching the legacy FederatedAnnotationReader behavior)
                final Map<String,AnnotationReader> readers = new HashMap<>();
                readers.put("annotation", annotationDao);
                readers.put("truthmark", truthmarkDao);

                annotationDataAccess = new FederatedAnnotationReader(readers, federatedReadExecutorService);
                log.debug("Annotation data access layer initialized successfully with federated reads (annotation + truthmark)");
            }
            return annotationDataAccess;
        }

        /**
         * Return the accumulo client currently held by this class. If there's a problem returning the client, logs a warning. Sets the internal client state to
         * null.
         */
        protected void returnAccumuloClient() {
            try {
                log.trace("Returning accumulo client");
                connectionFactory.returnClient(client);
                log.debug("Accumulo client returned");
            } catch (Exception e) {
                log.warn("Error when returning client", e);
            } finally {
                client = null;
            }
        }

        /**
         * Lookup an annotation source or retrieve it from the cache.
         *
         * @param analyticHash
         *            the analytic source hash to resolve
         * @return the cached or retrieved annotation source for the supplied hash, if one exists
         */
        public Optional<AnnotationSource> getAnnotationSource(String analyticHash) {
            return retrievedSourcesCache.computeIfAbsent(analyticHash, key -> annotationDataAccess.getAnnotationSource(analyticHash));
        }
    }
}
