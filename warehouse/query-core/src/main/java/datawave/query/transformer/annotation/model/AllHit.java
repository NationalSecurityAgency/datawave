package datawave.query.transformer.annotation.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import datawave.annotation.protobuf.v1.SegmentBoundary;

public class AllHit {
    @JsonProperty
    private float confidence;

    @JsonProperty
    private List<Term> oneBestContext = new ArrayList<>();

    @JsonProperty
    private List<TermHit> termHits = new ArrayList<>();

    @JsonIgnore
    private SegmentBoundary hitBoundary;

    public List<Term> getOneBestContext() {
        return oneBestContext;
    }

    public List<TermHit> getTermHits() {
        return termHits;
    }

    public float getConfidence() {
        return confidence;
    }

    public void setConfidence(float confidence) {
        this.confidence = confidence;
    }

    public SegmentBoundary getHitBoundary() {
        return hitBoundary;
    }

    public void setHitBoundary(SegmentBoundary hitBoundary) {
        this.hitBoundary = hitBoundary;
    }
}
