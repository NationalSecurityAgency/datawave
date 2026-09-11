package datawave.ingest.mapreduce.handler.edge.define;

import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.marking.Markings;

/**
 * Extracts the duration field specified by a Normalized Content Interface
 *
 *
 *
 */
public class DurationValue {
    private int duration = -1;

    private Markings<?> markings = null;

    public DurationValue(NormalizedContentInterface elapsedTimeNCI) {
        String durString = elapsedTimeNCI.getIndexedFieldValue();
        this.duration = Integer.parseInt(durString);
        this.markings = elapsedTimeNCI.getMarkings();
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

    public Markings<?> getMarkings() {
        return this.markings;
    }
}
