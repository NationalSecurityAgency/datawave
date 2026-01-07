package datawave.query.config.annotation;

import java.util.Set;

import datawave.query.transformer.annotation.AllHitsFactory;

public class AllHitsQueryConfig {

    /**
     * Enable the all hits transformer
     */
    private boolean annotationHitsEnabled = false;

    /**
     * Max number of terms to buffer when fetching all hits before and after the target term
     */
    private int annotationHitsMaxContextLength = 25;

    /**
     * annotation types to be used with all hits
     */
    private Set<String> annotationHitsValidTypes;

    /**
     * fields from the query that should be searched for all hits
     */
    private Set<String> annotationHitsValidQueryFields;

    /**
     * field to write all hits data to
     */
    private String annotationHitsTargetField = "ALL_HITS";

    private String annotationHitsFactoryClass = AllHitsFactory.class.getCanonicalName();

    private AnnotationConfig annotationConfig;

    public AllHitsQueryConfig() {

    }

    public AllHitsQueryConfig(AllHitsQueryConfig other) {
        setAnnotationHitsEnabled(other.isAnnotationHitsEnabled());
        setAnnotationHitsMaxContextLength(other.getAnnotationHitsMaxContextLength());
        setAnnotationHitsValidQueryFields(other.getAnnotationHitsValidQueryFields());
        setAnnotationHitsValidTypes(other.getAnnotationHitsValidTypes());
        setAnnotationHitsFactoryClass(other.getAnnotationHitsFactoryClass());
        setAnnotationHitsTargetField(other.getAnnotationHitsTargetField());
        setAnnotationConfig(other.getAnnotationConfig());
    }

    public boolean isAnnotationHitsEnabled() {
        return annotationHitsEnabled;
    }

    public void setAnnotationHitsEnabled(boolean annotationHitsEnabled) {
        this.annotationHitsEnabled = annotationHitsEnabled;
    }

    public int getAnnotationHitsMaxContextLength() {
        return annotationHitsMaxContextLength;
    }

    public void setAnnotationHitsMaxContextLength(int annotationHitsMaxContextLength) {
        this.annotationHitsMaxContextLength = annotationHitsMaxContextLength;
    }

    public Set<String> getAnnotationHitsValidTypes() {
        return annotationHitsValidTypes;
    }

    public void setAnnotationHitsValidTypes(Set<String> annotationHitsValidTypes) {
        this.annotationHitsValidTypes = annotationHitsValidTypes;
    }

    public Set<String> getAnnotationHitsValidQueryFields() {
        return annotationHitsValidQueryFields;
    }

    public void setAnnotationHitsValidQueryFields(Set<String> annotationHitsValidQueryFields) {
        this.annotationHitsValidQueryFields = annotationHitsValidQueryFields;
    }

    public String getAnnotationHitsTargetField() {
        return annotationHitsTargetField;
    }

    public void setAnnotationHitsTargetField(String annotationHitsTargetField) {
        this.annotationHitsTargetField = annotationHitsTargetField;
    }

    public String getAnnotationHitsFactoryClass() {
        return annotationHitsFactoryClass;
    }

    public void setAnnotationHitsFactoryClass(String annotationHitsFactoryClass) {
        this.annotationHitsFactoryClass = annotationHitsFactoryClass;
    }

    public AnnotationConfig getAnnotationConfig() {
        return annotationConfig;
    }

    public void setAnnotationConfig(AnnotationConfig annotationConfig) {
        this.annotationConfig = annotationConfig;
    }
}
