package datawave.ingest.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.metric.IngestInput;
import datawave.ingest.time.Now;

public class DataTypeDiscardFutureIntervalPredicate implements RawRecordPredicate {

    private static final Logger log = Logger.getLogger(DataTypeDiscardIntervalPredicate.class);

    /**
     * number which will be used to evaluate whether or not an Event should be processed. If the Event.getEventDate() is less than (now + interval) then it will
     * be processed.
     */
    public static final String DISCARD_FUTURE_INTERVAL = "event.discard.future.interval";

    private static final Now now = Now.getInstance();

    private long discardFutureInterval = 0L;

    @Override
    public void setConfiguration(String type, Configuration conf) {
        long defaultInterval = conf.getLong(DISCARD_FUTURE_INTERVAL, 0l);
        this.discardFutureInterval = conf.getLong(type + "." + DISCARD_FUTURE_INTERVAL, defaultInterval);
        log.info("Setting up type: " + type + " with future interval " + this.discardFutureInterval);
    }

    @Override
    public boolean shouldProcess(RawRecordContainer record) {
        // Determine whether the event date is greater than the interval. Excluding fatal error events.
        if (discardFutureInterval != 0L && (record.getDate() > (now.get() + discardFutureInterval))) {
            if (log.isInfoEnabled())
                log.info("Event with time " + record.getDate() + " newer than specified interval of " + (now.get() + discardFutureInterval) + ", skipping...");
            return false;
        }
        return true;
    }

    @Override
    public String getCounterName() {
        return IngestInput.FUTURE_EVENT.name();
    }

    @Override
    public int hashCode() {
        return (int) discardFutureInterval;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DataTypeDiscardFutureIntervalPredicate) {
            return discardFutureInterval == (((DataTypeDiscardFutureIntervalPredicate) obj).discardFutureInterval);
        }
        return false;
    }
}
