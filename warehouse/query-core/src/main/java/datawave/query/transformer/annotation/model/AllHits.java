package datawave.query.transformer.annotation.model;

import java.util.ArrayList;
import java.util.List;

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
}
