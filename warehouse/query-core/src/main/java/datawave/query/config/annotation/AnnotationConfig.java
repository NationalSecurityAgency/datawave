package datawave.query.config.annotation;

import java.io.Serializable;

import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;

public class AnnotationConfig implements Serializable {
    private String annotationTableName = "annotation";
    private String annotationSourceTableName = "annotationSource";
    private VisibilityTransformer visibilityTransformer;
    private TimestampTransformer timestampTransformer;

    public AnnotationConfig() {

    }

    public AnnotationConfig(AnnotationConfig other) {
        setAnnotationTableName(other.getAnnotationTableName());
        setAnnotationSourceTableName(other.getAnnotationSourceTableName());
        setVisibilityTransformer(other.getVisibilityTransformer());
        setTimestampTransformer(other.getTimestampTransformer());
    }

    public String getAnnotationTableName() {
        return annotationTableName;
    }

    public void setAnnotationTableName(String annotationTableName) {
        this.annotationTableName = annotationTableName;
    }

    public String getAnnotationSourceTableName() {
        return annotationSourceTableName;
    }

    public void setAnnotationSourceTableName(String annotationSourceTableName) {
        this.annotationSourceTableName = annotationSourceTableName;
    }

    public VisibilityTransformer getVisibilityTransformer() {
        return visibilityTransformer;
    }

    public void setVisibilityTransformer(VisibilityTransformer visibilityTransformer) {
        this.visibilityTransformer = visibilityTransformer;
    }

    public TimestampTransformer getTimestampTransformer() {
        return timestampTransformer;
    }

    public void setTimestampTransformer(TimestampTransformer timestampTransformer) {
        this.timestampTransformer = timestampTransformer;
    }
}
