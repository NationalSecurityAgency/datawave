package datawave.query.transformer.annotation.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TimeRange {
    @JsonProperty
    private float startTime;

    @JsonProperty
    private float endTime;

    public float getEndTime() {
        return endTime;
    }

    public void setEndTime(float endTime) {
        this.endTime = endTime;
    }

    public float getStartTime() {
        return startTime;
    }

    public void setStartTime(float startTime) {
        this.startTime = startTime;
    }
}
