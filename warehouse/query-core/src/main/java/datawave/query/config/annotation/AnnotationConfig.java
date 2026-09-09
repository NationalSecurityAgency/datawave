package datawave.query.config.annotation;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;

public class AnnotationConfig implements Serializable {
    private String annotationTableName = "annotation";
    private String annotationSourceTableName = "annotationSource";
    private String truthmarkTableName = "truthmark";
    private String truthmarkSourceTableName = "truthmarkSource";
    private VisibilityTransformer visibilityTransformer;
    private TimestampTransformer timestampTransformer;
    private List<String> maskSourceMetadata = List.of("visibility");

    public AnnotationConfig() {

    }

    public AnnotationConfig(AnnotationConfig other) {
        setAnnotationTableName(other.getAnnotationTableName());
        setAnnotationSourceTableName(other.getAnnotationSourceTableName());
        setTruthmarkTableName(other.getTruthmarkTableName());
        setTruthmarkSourceTableName(other.getTruthmarkSourceTableName());
        setVisibilityTransformer(other.getVisibilityTransformer());
        setTimestampTransformer(other.getTimestampTransformer());
        setMaskSourceMetadata(other.getMaskSourceMetadata());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof AnnotationConfig)) {
            return false;
        }

        // @formatter:off
        return Objects.equals(getAnnotationTableName(), ((AnnotationConfig) other).getAnnotationTableName()) &&
                Objects.equals(getAnnotationSourceTableName(), ((AnnotationConfig) other).getAnnotationSourceTableName()) &&
                Objects.equals(getTruthmarkTableName(), ((AnnotationConfig) other).getTruthmarkTableName()) &&
                Objects.equals(getTruthmarkSourceTableName(), ((AnnotationConfig) other).getTruthmarkSourceTableName()) &&
                Objects.equals(getVisibilityTransformer(), ((AnnotationConfig) other).getVisibilityTransformer()) &&
                Objects.equals(getTimestampTransformer(), ((AnnotationConfig) other).getTimestampTransformer()) &&
                Objects.equals(getMaskSourceMetadata(), ((AnnotationConfig) other).getMaskSourceMetadata());
        // @formatter:on
    }

    @Override
    public int hashCode() {
        // formatter:off
        return Objects.hash(getAnnotationTableName(), getAnnotationSourceTableName(), getTruthmarkTableName(), getTruthmarkSourceTableName(),
                        getVisibilityTransformer(), getTimestampTransformer(), getMaskSourceMetadata());
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

    public String getTruthmarkSourceTableName() {
        return truthmarkSourceTableName;
    }

    public void setTruthmarkSourceTableName(String truthmarkSourceTableName) {
        this.truthmarkSourceTableName = truthmarkSourceTableName;
    }

    public String getTruthmarkTableName() {
        return truthmarkTableName;
    }

    public void setTruthmarkTableName(String truthmarkTableName) {
        this.truthmarkTableName = truthmarkTableName;
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

    public List<String> getMaskSourceMetadata() {
        return maskSourceMetadata;
    }

    public void setMaskSourceMetadata(List<String> maskSourceMetadata) {
        this.maskSourceMetadata = maskSourceMetadata;
    }
}
