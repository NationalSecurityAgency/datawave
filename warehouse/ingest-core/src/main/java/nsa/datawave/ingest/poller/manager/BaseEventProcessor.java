package nsa.datawave.ingest.poller.manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

import nsa.datawave.ingest.data.RawRecordContainer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public abstract class BaseEventProcessor extends ArrayBlockingQueue<RawRecordContainer> implements PollerEventProcessor {
    
    public static class Counter {
        private long processed = 0;
        private long eventsSent = 0;
        private long ignoredDueToDiscardInterval = 0;
        private long ignoredDueToWrongDatatype = 0;
        private long messagesSent = 0;
        private long eventsReceived = 0;
        private long eventsDropped = 0;
        
        public void incrementProcessed() {
            this.processed++;
        }
        
        public void incrementIgnoredDueToDiscardInterval() {
            this.ignoredDueToDiscardInterval++;
        }
        
        public void incrementIgnoredDueToWrongDatatype() {
            this.ignoredDueToWrongDatatype++;
        }
        
        public void addSent(int sent) {
            this.eventsSent += sent;
        }
        
        @Override
        public String toString() {
            return String.format(
                            "received (events): %s, dropped (events) %s, accepted (events): %s, ignored (discard): %s, ignored (datatype): %s, sent (events): %s, sent (messages): %s\n",
                            eventsReceived, eventsDropped, processed, ignoredDueToDiscardInterval, ignoredDueToWrongDatatype, eventsSent, messagesSent);
        }
        
        public void incrementMessagesSent() {
            this.messagesSent++;
        }
        
        public void incrementReceived() {
            this.eventsReceived++;
        }
        
        public void incrementDropped() {
            this.eventsDropped++;
        }
    }
    
    private static final long serialVersionUID = 1L;
    private static final Logger log = Logger.getLogger(BaseEventProcessor.class);
    protected static final int DEFAULT_CAPACITY = 1000;
    
    protected SendThread sender = null;
    protected Object lock = new Object();
    private Option discardIntervalOption;
    private int discardIntervalDays = 2;
    private static final String DATATYPES_WHITELIST_CONF = ".datatypes.whitelist";
    private String[] dataTypesWhitelist;
    
    protected Counter counter = new Counter();
    
    public abstract class SendThread extends Thread {
        public abstract void shutdown();
    }
    
    public BaseEventProcessor() {
        super(DEFAULT_CAPACITY);
        sender = getSendThread();
    }
    
    /**
     * Attempts to add the event to the queue to send to NiFi, will not block.
     * 
     * @param e
     *            event
     */
    public void process(RawRecordContainer e) {
        
        RawRecordContainer event = e.copy();
        counter.incrementReceived();
        long discardDays = (this.discardIntervalDays * 86400000);
        long discardInterval = System.currentTimeMillis() - discardDays;
        if (event.getDate() < discardInterval) {
            counter.incrementIgnoredDueToDiscardInterval();
            return;
        }
        
        // check if the datatype being processed is allowed.
        boolean allowableDatatype = false;
        if (null != dataTypesWhitelist) {
            for (String datatype : dataTypesWhitelist) {
                if (event.getDataType().outputName().startsWith(datatype)) {
                    allowableDatatype = true;
                    break;
                }
            }
        }
        if (!allowableDatatype) {
            counter.incrementIgnoredDueToWrongDatatype();
            return;
        }
        
        if (!offer(event)) {
            log.warn("queue was full, need to lower the timeout or check NiFi process");
            counter.incrementDropped();
        } else {
            counter.incrementProcessed();
        }
        
        if (size() > (DEFAULT_CAPACITY / 4)) {
            synchronized (lock) {
                lock.notify();
            }
        }
    }
    
    public void close() {
        sender.shutdown();
        synchronized (lock) {
            lock.notify();
        }
        try {
            sender.join();
        } catch (InterruptedException e) {}
        clear();
    }
    
    @Override
    public void finishFile() {
        // Nothing to do here
    }
    
    public void configure(CommandLine cl, Configuration config) throws Exception {
        if (!cl.hasOption(discardIntervalOption.getOpt())) {
            throw new MissingOptionException(Collections.singletonList(discardIntervalOption));
        }
        this.discardIntervalDays = Integer.parseInt(cl.getOptionValue(discardIntervalOption.getOpt()));
        
        // *Note* if no whitelist is configured, then all events will be ignored by default
        dataTypesWhitelist = config.getStrings(getClass().getCanonicalName() + DATATYPES_WHITELIST_CONF);
        
        _configure(cl);
        sender.start();
    }
    
    public abstract void _configure(CommandLine cl) throws Exception;
    
    public Collection<Option> getConfigurationOptions() {
        ArrayList<Option> o = new ArrayList<>();
        
        discardIntervalOption = new Option("di", "discardInterval", true, "number of days of data to process");
        discardIntervalOption.setRequired(true);
        discardIntervalOption.setArgs(1);
        discardIntervalOption.setType(Integer.class);
        o.add(discardIntervalOption);
        
        return o;
    }
    
    protected abstract SendThread getSendThread();
    
}
