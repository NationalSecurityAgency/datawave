package datawave.query.transformer.annotation.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AllHit {
    @JsonProperty
    private float confidence;

    @JsonProperty
    private List<Term> context = new ArrayList<>();

    public List<Term> getContext() {
        return context;
    }

    public float getConfidence() {
        return confidence;
    }

    public void setConfidence(float confidence) {
        this.confidence = confidence;
    }
}
