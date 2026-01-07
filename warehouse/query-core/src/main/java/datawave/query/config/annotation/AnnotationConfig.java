package datawave.query.config.annotation;

public class AnnotationConfig {
    private String annotationTableName = "annotation";
    private String annotationSourceTableName = "annotationSource";

    public AnnotationConfig() {

    }

    public AnnotationConfig(AnnotationConfig other) {
        setAnnotationTableName(other.getAnnotationTableName());
        setAnnotationSourceTableName(other.getAnnotationSourceTableName());
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

}
