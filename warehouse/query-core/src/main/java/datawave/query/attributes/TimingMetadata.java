package datawave.query.attributes;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Holds timing information for query iterator next, source, seek, and yield counts.
 */
public class TimingMetadata extends Metadata {

    private static final String NEXT_COUNT = "NEXT_COUNT";
    private static final String SOURCE_COUNT = "SOURCE_COUNT";
    private static final String SEEK_COUNT = "SEEK_COUNT";
    private static final String YIELD_COUNT = "YIELD_COUNT";
    private static final String STAGE_TIMERS = "STAGE_TIMERS";
    private static final String HOST = "HOST";

    public long getNextCount() {
        Numeric numericValue = (Numeric) get(NEXT_COUNT);
        if (numericValue != null) {
            return ((Number) numericValue.getData()).longValue();
        } else {
            return 0;
        }
    }

    public void setNextCount(long nextCount) {
        put(NEXT_COUNT, new Numeric(nextCount, this.getMetadata(), this.isToKeep()));

    }

    public long getSourceCount() {
        Numeric numericValue = (Numeric) get(SOURCE_COUNT);
        if (numericValue != null) {
            return ((Number) numericValue.getData()).longValue();
        } else {
            return 0;
        }
    }

    public void setSourceCount(long sourceCount) {
        put(SOURCE_COUNT, new Numeric(sourceCount, this.getMetadata(), this.isToKeep()));
    }

    public long getSeekCount() {
        Numeric numericValue = (Numeric) get(SEEK_COUNT);
        if (numericValue != null) {
            return ((Number) numericValue.getData()).longValue();
        } else {
            return 0;
        }
    }

    public void setSeekCount(long seekCount) {
        put(SEEK_COUNT, new Numeric(seekCount, this.getMetadata(), this.isToKeep()));
    }

    public long getYieldCount() {
        Numeric numericValue = (Numeric) get(YIELD_COUNT);
        if (numericValue != null) {
            return ((Number) numericValue.getData()).longValue();
        } else {
            return 0L;
        }
    }

    public void setYieldCount(long yieldCount) {
        put(YIELD_COUNT, new Numeric(yieldCount, this.getMetadata(), this.isToKeep()));
    }

    public void addStageTimer(String stageName, Numeric elapsed) {
        Metadata stageTimers = (Metadata) get(STAGE_TIMERS);
        if (stageTimers == null) {
            stageTimers = new Metadata();
            put(STAGE_TIMERS, stageTimers);
        }
        stageTimers.put(stageName, elapsed);
        put(STAGE_TIMERS, stageTimers);
    }

    public Map<String,Long> getStageTimers() {
        Map<String,Long> stageTimers = new LinkedHashMap<>();
        Attribute stageTimersAttribute = get(STAGE_TIMERS);
        if (stageTimersAttribute instanceof Metadata) {
            Metadata stageTimersMetadata = (Metadata) stageTimersAttribute;
            for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : stageTimersMetadata.entrySet()) {
                if (entry.getValue() instanceof Numeric) {
                    Number value = (Number) entry.getValue().getData();
                    stageTimers.put(entry.getKey(), value.longValue());
                }
            }
        }
        return stageTimers;
    }

    public String getHost() {
        Attribute hostAttribute = get(HOST);
        if (hostAttribute instanceof Content) {
            return ((Content) hostAttribute).getContent();
        } else {
            return null;
        }
    }

    public void setHost(String host) {
        put(HOST, new Content(host, this.getMetadata(), this.isToKeep()));
    }
}
