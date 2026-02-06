package datawave.query.config.annotation;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import datawave.data.normalizer.Normalizer;
import datawave.query.transformer.annotation.AllHitsFactory;
import datawave.query.transformer.annotation.TermExtractor;

public class AllHitsQueryConfig implements Serializable {

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
     * field to write all hits data to
     */
    private String targetField = "ALL_HITS";

    private String allHitsFactoryClass = AllHitsFactory.class.getCanonicalName();

    private TermExtractor queryTermExtractor;

    private Normalizer<String> termNormalizer;

    /**
     * A mapping between fields in the event associating an annotation to a field to be returned in an AllHits dynamic field.
     */
    private Map<String,String> annotationEnrichmentFieldMap = new HashMap<>();

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
        setTermNormalizer(other.getTermNormalizer());
        setAnnotationEnrichmentFieldMap(other.getAnnotationEnrichmentFieldMap());
        setAnnotationConfig(other.getAnnotationConfig());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof AllHitsQueryConfig)) {
            return false;
        }

        AllHitsQueryConfig that = (AllHitsQueryConfig) other;

        // @formatter:off
        return Objects.equals(isEnabled(), that.isEnabled()) &&
                Objects.equals(getMaxContextLength(), that.getMaxContextLength()) &&
                Objects.equals(getValidAnnotationTypes(), that.getValidAnnotationTypes()) &&
                Objects.equals(getAllHitsFactoryClass(), that.getAllHitsFactoryClass()) &&
                Objects.equals(getTargetField(), that.getTargetField()) &&
                Objects.equals(getQueryTermExtractor(), that.getQueryTermExtractor()) &&
                Objects.equals(getTermNormalizer(), that.getTermNormalizer()) &&
                Objects.equals(getAnnotationEnrichmentFieldMap(), that.getAnnotationEnrichmentFieldMap()) &&
                Objects.equals(getAnnotationConfig(), that.getAnnotationConfig());
        // @formatter:on
    }

    @Override
    public int hashCode() {
        // @formatter:off
        return Objects.hash(
                isEnabled(),
                getMaxContextLength(),
                getValidAnnotationTypes(),
                getAllHitsFactoryClass(),
                getTargetField(),
                getQueryTermExtractor(),
                getTermNormalizer(),
                getAnnotationEnrichmentFieldMap(),
                getAnnotationConfig()
        );
        // @formatter:on
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

    public Normalizer<String> getTermNormalizer() {
        return termNormalizer;
    }

    public void setTermNormalizer(Normalizer<String> termNormalizer) {
        this.termNormalizer = termNormalizer;
    }

    public Map<String,String> getAnnotationEnrichmentFieldMap() {
        return annotationEnrichmentFieldMap;
    }

    public void setAnnotationEnrichmentFieldMap(Map<String,String> annotationEnrichmentFieldMap) {
        this.annotationEnrichmentFieldMap = annotationEnrichmentFieldMap;
    }
}
