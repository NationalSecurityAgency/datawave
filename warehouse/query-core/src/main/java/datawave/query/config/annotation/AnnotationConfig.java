package datawave.query.config.annotation;

import java.io.Serializable;
import java.util.Objects;

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

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof AnnotationConfig)) {
            return false;
        }

        // @formatter:off
        return Objects.equals(getAnnotationTableName(), ((AnnotationConfig) other).getAnnotationTableName()) &&
                Objects.equals(getAnnotationSourceTableName(), ((AnnotationConfig) other).getAnnotationSourceTableName()) &&
                Objects.equals(getVisibilityTransformer(), ((AnnotationConfig) other).getVisibilityTransformer()) &&
                Objects.equals(getTimestampTransformer(), ((AnnotationConfig) other).getTimestampTransformer());
        // @formatter:on
    }

    @Override
    public int hashCode() {
        // formatter:off
        return Objects.hash(getAnnotationTableName(), getAnnotationSourceTableName(), getVisibilityTransformer(), getTimestampTransformer());
        // formatter:on
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
