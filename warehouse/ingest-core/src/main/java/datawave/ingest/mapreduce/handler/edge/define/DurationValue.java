package datawave.ingest.mapreduce.handler.edge.define;

import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.ingest.data.config.NormalizedContentInterface;

/**
 * Extracts the duration field specified by a Normalized Content Interface
 *
 *
 *
 */
public class DurationValue {
    private int duration = -1;

    private ColumnVisibility visibility = null;

    public DurationValue(NormalizedContentInterface elapsedTimeNCI) {
        String durString = elapsedTimeNCI.getIndexedFieldValue();
        this.duration = Integer.parseInt(durString);
        this.visibility = elapsedTimeNCI.getVisibility();
    }

    public DurationValue(NormalizedContentInterface uptimeNCI, NormalizedContentInterface downtimeNCI) {
        String uptimeString = uptimeNCI.getIndexedFieldValue();
        String downtimeString = downtimeNCI.getIndexedFieldValue();
        Long duration = (Long.parseLong(downtimeString) - Long.parseLong(uptimeString)) / 1000;
        this.duration = duration.intValue();
    }

    public int getDuration() {
        if (this.duration < 0) {
            return -1;
        } else {
            return this.duration;
        }
    }

    /**
     * @return the field specific visibility of the duration value, or null if the event visibility applies
     */
    public ColumnVisibility getVisibility() {
        return this.visibility;
    }
}
