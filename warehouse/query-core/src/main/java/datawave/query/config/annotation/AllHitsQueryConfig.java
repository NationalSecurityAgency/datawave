package datawave.query.config.annotation;

import java.util.Set;

import datawave.query.transformer.annotation.AllHitsFactory;
import datawave.query.transformer.annotation.TermExtractor;

public class AllHitsQueryConfig {

    /**
     * Enable the all hits transformer
     */
    private boolean enabled = false;

    /**
     * Max number of terms to buffer when fetching all hits before and after the target term
     */
    private int maxContextLength = 25;

    /**
     * annotation types to be used with all hits
     */
    private Set<String> validAnnotationTypes;

    /**
     * fields from the query that should be searched for all hits
     */
    private Set<String> validQueryFields;

    /**
     * field to write all hits data to
     */
    private String targetField = "ALL_HITS";

    private String allHitsFactoryClass = AllHitsFactory.class.getCanonicalName();

    private TermExtractor queryTermExtractor;

    private AnnotationConfig annotationConfig;

    public AllHitsQueryConfig() {

    }

    public AllHitsQueryConfig(AllHitsQueryConfig other) {
        setEnabled(other.isEnabled());
        setMaxContextLength(other.getMaxContextLength());
        setValidAnnotationTypes(other.getValidAnnotationTypes());
        setAllHitsFactoryClass(other.getAllHitsFactoryClass());
        setTargetField(other.getTargetField());
        setQueryTermExtractor(other.getQueryTermExtractor());
        setAnnotationConfig(other.getAnnotationConfig());
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getMaxContextLength() {
        return maxContextLength;
    }

    public void setMaxContextLength(int maxContextLength) {
        this.maxContextLength = maxContextLength;
    }

    public Set<String> getValidAnnotationTypes() {
        return validAnnotationTypes;
    }

    public void setValidAnnotationTypes(Set<String> validAnnotationTypes) {
        this.validAnnotationTypes = validAnnotationTypes;
    }

    public String getTargetField() {
        return targetField;
    }

    public void setTargetField(String targetField) {
        this.targetField = targetField;
    }

    public String getAllHitsFactoryClass() {
        return allHitsFactoryClass;
    }

    public void setAllHitsFactoryClass(String allHitsFactoryClass) {
        this.allHitsFactoryClass = allHitsFactoryClass;
    }

    public AnnotationConfig getAnnotationConfig() {
        return annotationConfig;
    }

    public void setAnnotationConfig(AnnotationConfig annotationConfig) {
        this.annotationConfig = annotationConfig;
    }

    public TermExtractor getQueryTermExtractor() {
        return queryTermExtractor;
    }

    public void setQueryTermExtractor(TermExtractor queryTermExtractor) {
        this.queryTermExtractor = queryTermExtractor;
    }
}
