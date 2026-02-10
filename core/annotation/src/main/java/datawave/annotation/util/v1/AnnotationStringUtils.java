package datawave.annotation.util.v1;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;

/** Utility methods that provide useful tools to convert protobuf generated classes to strings */
public class AnnotationStringUtils {
    public static String annotationSourceString(AnnotationSource s) {
        return "AnnotationSource[" + "analyticSourceHash=" + s.getAnalyticSourceHash() + " analyticHash=" + s.getAnalyticHash() + " engine=" + s.getEngine()
                        + " model=" + s.getModel() + "]";
    }

    public static String annotationIdContext(Annotation a) {
        return "'" + a.getAnnotationId() + "', annotation context was: " + a.getShard() + "/" + a.getDataType() + "/" + a.getUid();
    }

    public static String segmentWithAnnotationContext(Segment s, Annotation a) {
        return "'" + s + "', annotation context was: " + a.getShard() + "/" + a.getDataType() + "/" + a.getUid();
    }
}
