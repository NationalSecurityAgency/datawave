package datawave.query.transformer.annotation.model;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName(value = "allHits")
public class AllHits {
    @JsonProperty
    private String annotationId;

    @JsonProperty
    private float maxTermHitConfidence;

    @JsonProperty
    private List<AllHit> keywordResultList = new ArrayList<>();

    private Map<String,String> dynamicProperties = new HashMap<>();

    public List<AllHit> getKeywordResultList() {
        return keywordResultList;
    }

    public float getMaxTermHitConfidence() {
        return maxTermHitConfidence;
    }

    public void setMaxTermHitConfidence(float maxTermHitConfidence) {
        this.maxTermHitConfidence = maxTermHitConfidence;
    }

    public String getAnnotationId() {
        return annotationId;
    }

    public void setAnnotationId(String annotationId) {
        this.annotationId = annotationId;
    }

    @JsonAnyGetter
    public Map<String,String> getDynamicProperties() {
        return dynamicProperties;
    }

    @JsonAnySetter
    public void addDynamicProperties(String key, String value) {
        for (Field field : this.getClass().getDeclaredFields()) {
            if (key.equals(field.getName())) {
                throw new IllegalArgumentException("cannot add dynamic property that matches field name: " + key);
            }
        }
        this.dynamicProperties.put(key, value);
    }
}
