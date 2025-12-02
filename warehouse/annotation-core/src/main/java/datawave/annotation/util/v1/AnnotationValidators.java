package datawave.annotation.util.v1;

import org.apache.commons.lang3.StringUtils;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.Validator;

/** Utility class holding validator implementations for Annotation and Segment classes */
public class AnnotationValidators {
    //@formatter:off
    public static final Validator<Annotation> annotationValidator = Validator.<Annotation>create()
            .addCheck(a -> StringUtils.isNotBlank(a.getShard()), "Annotation shard must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getDataType()), "Annotation datatype must not be blank")
            .addCheck(a -> StringUtils.isNotBlank(a.getUid()), "Annotation uid must not be blank")
            .addCheck(a -> !a.getMetadataMap().isEmpty(), "Metadata map must include a 'visibility' and 'created_date'")
            .addCheck(a -> a.getMetadataMap().get("visibility") != null, "Annotation Metadata map must include a 'visibility'")
            .addCheck(a -> a.getMetadataMap().get("created_date") != null, "Annotation Metadata map must include a 'created_date'")
            .addCheck(a -> !a.getSegmentsList().isEmpty(), "Annotation must include at least one segment");

    public static final Validator<Segment> segmentValidator = Validator.<Segment>create()
            .addCheck(s -> !s.getSegmentValueList().isEmpty(), "Segment must have at lease one value");
    //@formatter:on

    public static Validator<Annotation> getAnnotationValidator() {
        return annotationValidator;
    }

    public static Validator<Segment> getSegmentValidator() {
        return segmentValidator;
    }
}
