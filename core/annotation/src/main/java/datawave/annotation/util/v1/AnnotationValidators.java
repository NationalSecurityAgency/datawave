package datawave.annotation.util.v1;

import java.util.Collections;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.annotation.util.Validator;

/** Utility class holding validator implementations for Annotation and Segment classes */
public class AnnotationValidators {
    //@formatter:off
    private static final Validator<AnnotationSource> annotationSourceValidator = Validator.<AnnotationSource>create()
            .addCheck(a -> StringUtils.isNotBlank(a.getAnalyticSourceHash()), "Annotation source source-hash must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getAnalyticHash()), "Annotation source hash must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getEngine()), "Annotation source engine must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getModel()), "Annotation source model must not be blank")
            .addCheck(a -> !a.getMetadataMap().isEmpty(), "Annotation source metadata map must include a 'visibility' and 'created_date'")
            .addCheck(a -> a.getMetadataMap().get("visibility") != null, "Annotation source metadata map must include a 'visibility'")
            .addCheck(a -> a.getMetadataMap().get("created_date") != null, "Annotation source metadata map must include a 'created_date'");

    private static final Validator<SegmentValue> segmentValueValidator = Validator.<SegmentValue>create()
            .addCheck(v -> StringUtils.isNotBlank(v.getValue()), "Segment value must not be blank");

    private static final Validator<SegmentBoundary> allBoundaryValidator = Validator.<SegmentBoundary>create()
            .addCheck(b -> !b.hasEnd(), "All boundary should not have an end value")
            .addCheck(b -> !b.hasStart(), "All boundary should not have a start value")
            .addCheck(b -> b.getPointsList().isEmpty(), "All boundary must not contain points");

    private static final Validator<SegmentBoundary> spanBoundaryValidator = Validator.<SegmentBoundary>create()
            .addCheck(SegmentBoundary::hasEnd, "Span boundary must have a start value")
            .addCheck(SegmentBoundary::hasStart, "Span boundary must have an end value")
            .addCheck(b -> b.getPointsList().isEmpty(), "Span boundary must not contain points");

    private static final Validator<SegmentBoundary> pointsBoundaryValidator = Validator.<SegmentBoundary>create()
            .addCheck(b -> !b.hasEnd(), "Point boundary should not have an end value")
            .addCheck(b -> !b.hasStart(), "Point boundary should not have a start value")
            .addCheck(b -> !b.getPointsList().isEmpty(), "Point boundary list must contain at least one point");

    private static final Function<SegmentBoundary, Validator<SegmentBoundary>> getBoundaryValidatorByType = segmentBoundary -> {
        switch (segmentBoundary.getBoundaryType()) {
            case ALL:
                return allBoundaryValidator;
            case TIME_MILLI:
            case TEXT_CHAR:
                return spanBoundaryValidator;
            case POINTS:
                return pointsBoundaryValidator;
        }
        throw new IllegalArgumentException("Invalid boundary type for validation: " + segmentBoundary);
    };

    private static final Validator<Segment> segmentValidator = Validator.<Segment>create()
            .addCheck(s -> !s.getValuesList().isEmpty(), "Segment must have at least one value")
            .addCheck(Segment::hasBoundary, "Segment must have a boundary")
            .addMemberValidator(Segment::getValuesList, segmentValueValidator)
            .addMemberValidator(s -> Collections.singletonList(s.getBoundary()), getBoundaryValidatorByType);

    private static final Validator<Annotation> annotationValidator = Validator.<Annotation>create()
            .addCheck(a -> StringUtils.isNotBlank(a.getShard()), "Annotation shard must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getDataType()), "Annotation datatype must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getUid()), "Annotation uid must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getAnnotationId()), "Annotation id must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getDocumentId()), "Annotation document id must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getAnalyticSourceHash()) || StringUtils.isNotBlank(a.getSource().getAnalyticHash()), "Annotation source hash must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getAnnotationType()), "Annotation type must not be blank")
            .addCheck(a -> !a.getMetadataMap().isEmpty(), "Annotation metadata map must include a 'visibility' and 'created_date'")
            .addCheck(a -> a.getMetadataMap().get("visibility") != null, "Annotation metadata map must include a 'visibility'")
            .addCheck(a -> a.getMetadataMap().get("created_date") != null, "Annotation metadata map must include a 'created_date'")
            .addCheck(a -> !a.getSegmentsList().isEmpty(), "Annotation must include at least one segment")
            .addMemberValidator(Annotation::getSegmentsList, segmentValidator);
    //@formatter:on

    public static Validator.ValidationState<AnnotationSource> checkAnnotationSource(AnnotationSource annotation) {
        return annotationSourceValidator.check(annotation);
    }

    public static Validator.ValidationState<Annotation> checkAnnotation(Annotation annotation) {
        return annotationValidator.check(annotation);
    }

    public static Validator.ValidationState<Segment> checkSegment(Segment segment) {
        return segmentValidator.check(segment);
    }

    public static Validator<AnnotationSource> getAnnotationSourceValidator() {
        return annotationSourceValidator;
    }

    public static Validator<Annotation> getAnnotationValidator() {
        return annotationValidator;
    }

    public static Validator<Segment> getSegmentValidator() {
        return segmentValidator;
    }
}
