package nsa.datawave.poller.metric;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

public class FlowStatistics {
    private static final long UPTIME = System.currentTimeMillis();
    private static final int HOUR_IN_SECS = 60 * 60;
    private static final int DAY_IN_SECS = 24 * HOUR_IN_SECS;
    private static final long DAY_IN_MSECS = 1000L * DAY_IN_SECS;
    // stats to generate : 24 hrs, 12 hrs, 1 hr, 30 min, 10 min
    private static final Set<Integer> STATS_TO_GENERATE = new HashSet<>(Arrays.asList(new Integer[] {DAY_IN_SECS, DAY_IN_SECS / 2, HOUR_IN_SECS,
            HOUR_IN_SECS / 2, 10 * 60}));
    
    private long[] counts = new long[DAY_IN_SECS];
    private int start = 0;
    private long startTime = UPTIME - DAY_IN_MSECS;
    private boolean testing = false;
    private Timer statsTimer = null;
    private String statsType = "Flow";
    
    public FlowStatistics() {
        
    }
    
    public FlowStatistics(String type) {
        this.statsType = type;
    }
    
    /**
     * Increment the stats for the current time slot by the specified amount
     * 
     * @param count
     */
    public synchronized void incrementStats(int count) {
        long now = System.currentTimeMillis();
        
        // determine whether we need to shift the start
        shiftStart(now);
        
        // now increment the stat
        int nowIndex = (start + DAY_IN_SECS - 1) % DAY_IN_SECS;
        counts[nowIndex] += count;
    }
    
    /**
     * Shift the start if appropriate and zero out the obsolete counts
     * 
     * @param now
     */
    private synchronized void shiftStart(long now) {
        // delta is the number of seconds past one day since the start of the current stats
        long delta = (now - startTime - DAY_IN_MSECS);
        if (!testing) {
            delta = delta / 1000L;
        }
        if (delta > 0) {
            // if wrapping more than the size of the array, then, reset everything
            if (delta > DAY_IN_SECS) {
                Arrays.fill(counts, 0);
                start = 0;
            }
            // if wrapping over the end of the array, reset appropriately
            else if (delta >= (DAY_IN_SECS - start)) {
                Arrays.fill(counts, start, DAY_IN_SECS, 0);
                start = (int) (((long) start + delta) % DAY_IN_SECS);
                Arrays.fill(counts, 0, start, 0);
            }
            // if not wrapping, zero out appropriately
            else {
                Arrays.fill(counts, start, start + (int) delta, 0);
                start = start + (int) delta;
                if (start == DAY_IN_SECS) {
                    start = 0;
                }
            }
            // now reset the start time
            startTime = now - DAY_IN_MSECS;
        }
    }
    
    /**
     * Return the flow rates in messages per second over a set interval
     * 
     * @return Map of minute interval to rate in messages per second
     */
    public synchronized Map<Integer,Double> getStats() {
        long now = System.currentTimeMillis();
        
        // shift the array so that we only have the last 24 hours of stats
        shiftStart(now);
        
        // calculate how long we have been gathering stats
        long upTimeSecs = (now - UPTIME);
        if (!testing) {
            upTimeSecs = upTimeSecs / 1000L;
        }
        
        // create the array
        Map<Integer,Double> stats = new TreeMap<>();
        
        // keep a running total and count
        int count = 0;
        double sum = 0;
        
        // navigate the array starting with the most current stat and work backwords
        for (int i = start + DAY_IN_SECS - 1; i >= start; i--) {
            // if over the amount of stats gathered thus far, then drop out
            if (count > upTimeSecs) {
                break;
            }
            int index = i % DAY_IN_SECS;
            count++;
            sum += counts[index];
            if (STATS_TO_GENERATE.contains(count)) {
                stats.put(count, sum / count);
            }
        }
        return stats;
    }
    
    /**
     * Show the stats periodically
     * 
     * @param statsFrequency
     */
    public synchronized void showStats(long statsFrequency) {
        if (statsFrequency > 0) {
            shutdownStats();
            statsTimer = new Timer("FlowStatistics", true);
            statsTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    System.out.println(FlowStatistics.this);
                }
            }, statsFrequency, statsFrequency);
        } else {
            throw new IllegalArgumentException("Stats frequency must be greater than 0.");
        }
    }
    
    /*
     * Shutdown the statistics thread
     */
    public synchronized void shutdownStats() {
        if (statsTimer != null) {
            statsTimer.cancel();
            statsTimer = null;
        }
    }
    
    /**
     * Output the stats in a human readable form
     */
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(statsType).append(" Statistics since ").append(new Date());
        for (Map.Entry<Integer,Double> entry : getStats().entrySet()) {
            builder.append("; Past ");
            if (entry.getKey() >= HOUR_IN_SECS) {
                builder.append(entry.getKey() / HOUR_IN_SECS).append(" hours: ");
            } else if (entry.getKey() >= 60) {
                builder.append(entry.getKey() / 60).append(" minutes: ");
            } else {
                builder.append(entry.getKey()).append(" seconds: ");
            }
            builder.append(entry.getValue());
        }
        return builder.toString();
    }
    
    /**
     * A little testing to verify this classes output
     */
    public static void main(String[] args) {
        FlowStatistics stats = new FlowStatistics();
        stats.testing = true;
        stats.showStats(2000);
        // do this for so many milliseconds
        for (int i = 0; i < DAY_IN_SECS; i++) {
            stats.incrementStats(2);
            try {
                Thread.sleep(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
