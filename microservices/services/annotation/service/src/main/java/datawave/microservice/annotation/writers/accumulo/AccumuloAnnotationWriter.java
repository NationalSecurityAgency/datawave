package datawave.microservice.annotation.writers.accumulo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;

import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer;
import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.annotation.writers.AnnotationWriter;
import datawave.microservice.annotation.writers.accumulo.config.AccumuloAnnotationWriterProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AccumuloAnnotationWriter implements AnnotationWriter {
    // TODO: consider making these configurable
    static final String USER_DN = "AccumuloAnnotationWriter";
    static final String DEFAULT_POOL = "WAREHOUSE";
    static final Collection<String> EMPTY_PROXY_SERVERS = Collections.emptyList();

    @Getter
    private final ConcurrentHashMap<String,Long> writeTimers = new ConcurrentHashMap<>();

    private final AnnotationDataAccess annotationDataAccess;

    public AccumuloAnnotationWriter(AccumuloConnectionFactory connectionFactory, AccumuloAnnotationWriterProperties properties,
                    AccumuloAnnotationSerializer annotationSerializer, AccumuloAnnotationSourceSerializer annotationSourceSerializer) {
        // TODO: figure out connection management (e.g, configuration lifecycle..)
        Map<String,String> trackingMap = new HashMap<>();

        try {
            AccumuloClient accumuloClient = connectionFactory.getClient(USER_DN, EMPTY_PROXY_SERVERS, DEFAULT_POOL, AccumuloConnectionFactory.Priority.NORMAL,
                            trackingMap);
            String accumuloUser = accumuloClient.whoami();
            final Authorizations authorizations = accumuloClient.securityOperations().getUserAuthorizations(accumuloUser);
            log.info("Writing annotations as {}, with authorizations {}", accumuloUser, authorizations);
            final Set<Authorizations> authoriationsSet = Set.of(authorizations);

            if (!accumuloClient.tableOperations().exists(properties.getTruthmarkTableName()))
                accumuloClient.tableOperations().create(properties.getTruthmarkTableName());

            if (!accumuloClient.tableOperations().exists(properties.getTruthmarkSourceTableName()))
                accumuloClient.tableOperations().create(properties.getTruthmarkSourceTableName());

            this.annotationDataAccess = new AnnotationDataAccess(accumuloClient, authoriationsSet, properties.getTruthmarkTableName(),
                            properties.getTruthmarkSourceTableName(), annotationSerializer, annotationSourceSerializer);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public Optional<Annotation> write(Annotation annotation) {
        String annotationId = annotation.getAnnotationId();
        // save the start time of the write call
        writeTimers.put(annotationId, System.currentTimeMillis());
        try {
            AnnotationSource annotationSource = annotation.getSource();

            // let the data access layer assigne the annotation source identifiers.
            AnnotationSource finalSource = annotationSource.toBuilder().clearAnalyticSourceHash().clearAnalyticHash().build();
            Optional<AnnotationSource> sourceResult = annotationDataAccess.addAnnotationSource(finalSource);
            if (sourceResult.isEmpty()) {
                return Optional.empty();
            }
            AnnotationSource writtenSource = sourceResult.get();

            // clear the annotation segment hashes and let the data access layer assign them.
            List<Segment> segments = annotation.getSegmentsList();
            List<Segment> writtenSegments = new ArrayList<>();
            for (Segment segment : segments) {
                Segment writtenSegment = segment.toBuilder().clearSegmentHash().build();
                writtenSegments.add(writtenSegment);
            }

            // let the data access layer assign the annotation identifier.
            Annotation finalAnnotation = annotation.toBuilder().clearAnnotationId().clearSegments().addAllSegments(writtenSegments).build();

            // if this annotation carries a reference to another annotation it is updating, delegate to updateAnnotation so that the target
            // annotation's existence is verified and the linkage is preserved. Otherwise, treat this as a new annotation.
            String updateTargetId = annotation.getMetadataMap().get(AnnotationUtils.UPDATE_REFERENCE);
            Optional<Annotation> result = StringUtils.isNotBlank(updateTargetId) ? annotationDataAccess.updateAnnotation(updateTargetId, finalAnnotation)
                            : annotationDataAccess.addAnnotation(finalAnnotation);
            if (result.isEmpty()) {
                return result;
            }
            Annotation writtenAnnotation = result.get();

            // re-assign the written annotation source to the written annotation to be returned.
            Annotation updatedAnnotation = writtenAnnotation.toBuilder().clearAnalyticSourceHash().clearSource()
                            .setAnalyticSourceHash(writtenSource.getAnalyticSourceHash()).setSource(writtenSource).build();
            return Optional.of(updatedAnnotation);
        } finally {
            writeTimers.remove(annotationId);
        }
    }

    @VisibleForTesting
    public AnnotationDataAccess getDataAccess() {
        return annotationDataAccess;
    }
}
