package datawave.query.transformer.annotation.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TermHit {
    @JsonProperty
    private String label;

    @JsonProperty
    private float confidence;

    @JsonProperty
    private boolean oneBest;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public float getConfidence() {
        return confidence;
    }

    public void setConfidence(float confidence) {
        this.confidence = confidence;
    }

    public boolean isOneBest() {
        return oneBest;
    }

    public void setOneBest(boolean oneBest) {
        this.oneBest = oneBest;
    }
}
