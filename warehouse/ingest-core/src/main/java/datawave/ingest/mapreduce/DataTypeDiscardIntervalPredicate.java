package datawave.ingest.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.metric.IngestInput;
import datawave.ingest.time.Now;

public class DataTypeDiscardIntervalPredicate implements RawRecordPredicate {

    private static final Logger log = Logger.getLogger(DataTypeDiscardIntervalPredicate.class);

    /**
     * number which will be used to evaluate whether or not an Event should be processed. If the Event.getEventDate() is greater than (now - interval) then it
     * will be processed.
     */
    public static final String DISCARD_INTERVAL = "event.discard.interval";

    private static final Now now = Now.getInstance();

    private long discardInterval = 0L;

    @Override
    public void setConfiguration(String type, Configuration conf) {
        long defaultInterval = conf.getLong(DISCARD_INTERVAL, 0l);
        this.discardInterval = conf.getLong(type + "." + DISCARD_INTERVAL, defaultInterval);
        log.info("Setting up type: " + type + " with interval " + this.discardInterval);
    }

    @Override
    public boolean shouldProcess(RawRecordContainer record) {
        // Determine whether the event date is greater than the interval. Excluding fatal error events.
        if (discardInterval != 0L && (record.getDate() < (now.get() - discardInterval))) {
            if (log.isInfoEnabled())
                log.info("Event with time " + record.getDate() + " older than specified interval of " + (now.get() - discardInterval) + ", skipping...");
            return false;
        }
        return true;
    }

    @Override
    public String getCounterName() {
        return IngestInput.OLD_EVENT.name();
    }

    @Override
    public int hashCode() {
        return (int) discardInterval;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DataTypeDiscardIntervalPredicate) {
            return discardInterval == (((DataTypeDiscardIntervalPredicate) obj).discardInterval);
        }
        return false;
    }
}
