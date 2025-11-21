package datawave.annotation.util.v1;

import org.apache.commons.lang3.StringUtils;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
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
            .addCheck(a -> a.getMetadataMap().get("created_date") != null, "Annotation source metadata map must include a 'created_date'")
            .addCheck(a -> !a.getConfigurationMap().isEmpty(), "Annotation source configuration must include at least one entry");

    private static final Validator<Annotation> annotationValidator = Validator.<Annotation>create()
            .addCheck(a -> StringUtils.isNotBlank(a.getShard()), "Annotation shard must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getDataType()), "Annotation datatype must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getUid()), "Annotation uid must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getDocumentId()), "Annotation document id must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getAnalyticSourceHash()) || StringUtils.isNotBlank(a.getSource().getAnalyticHash()), "Annotation source hash must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getAnnotationType()), "Annotation type must not be blank")
            .addCheck(a -> !a.getMetadataMap().isEmpty(), "Annotation metadata map must include a 'visibility' and 'created_date'")
            .addCheck(a -> a.getMetadataMap().get("visibility") != null, "Annotation metadata map must include a 'visibility'")
            .addCheck(a -> a.getMetadataMap().get("created_date") != null, "Annotation metadata map must include a 'created_date'")
            .addCheck(a -> !a.getSegmentsList().isEmpty(), "Annotation must include at least one segment");

    private static final Validator<Segment> segmentValidator = Validator.<Segment>create()
            .addCheck(s -> !s.getValuesList().isEmpty(), "Segment must have at least one value");
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
