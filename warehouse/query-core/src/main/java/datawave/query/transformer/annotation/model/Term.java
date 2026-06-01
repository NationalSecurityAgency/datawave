package datawave.query.transformer.annotation.model;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Term {
    @JsonProperty
    private String label;

    @JsonProperty
    private float confidence;

    @JsonProperty
    private Set<TermHit> hits;

    @JsonProperty
    private TimeRange timeRange = new TimeRange();

    public TimeRange getTimeRange() {
        return timeRange;
    }

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

    public Set<TermHit> getHits() {
        return hits;
    }

    public void setHits(Set<TermHit> hits) {
        this.hits = hits;
    }
}
