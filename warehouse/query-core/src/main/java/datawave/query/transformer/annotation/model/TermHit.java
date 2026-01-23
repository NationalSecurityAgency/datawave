package datawave.query.transformer.annotation.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TermHit {
    @JsonProperty
    private String termLabel;

    @JsonProperty
    private float confidence;

    @JsonProperty
    private TimeRange timeRange = new TimeRange();

    public String getTermLabel() {
        return termLabel;
    }

    public void setTermLabel(String termLabel) {
        this.termLabel = termLabel;
    }

    public float getConfidence() {
        return confidence;
    }

    public void setConfidence(float confidence) {
        this.confidence = confidence;
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }
}
