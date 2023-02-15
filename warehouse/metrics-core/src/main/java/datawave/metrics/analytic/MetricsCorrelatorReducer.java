package datawave.metrics.analytic;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeSet;
import java.util.SortedSet;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

/**
 * Creates mutations for each data and ingest type, based on timestamp, for the duration of each ingest segment.
 * 
 */
public class MetricsCorrelatorReducer extends Reducer<Text,LongArrayWritable,Text,Mutation> {
    static Logger log = Logger.getLogger(MetricsCorrelatorReducer.class);
    
    private ByteArrayOutputStream valueBuffer = new ByteArrayOutputStream();
    
    @Override
    protected void reduce(Text timestampAndType, Iterable<LongArrayWritable> flows, Context context) throws IOException, InterruptedException {
        
        LongArrayWritable outVal;
        String keyIn = timestampAndType.toString();
        if (keyIn.endsWith("live")) {
            outVal = createSummary(flows, false);
        } else if (keyIn.endsWith("bulk")) {
            outVal = createSummary(flows, true);
        } else {
            return;
        }
        
        outVal.write(new DataOutputStream(valueBuffer));
        String[] key = timestampAndType.toString().split(":");
        Mutation m = new Mutation(key[0]);
        String metricsLabel = key[2];
        // if the metrics label is overridden, it will be something like this: fifteen\u0000live or proto\u0000bulk
        if (metricsLabel.contains("\u0000")) {
            metricsLabel = metricsLabel.split("\u0000")[0];
        }
        m.put(key[1], metricsLabel, new Value(valueBuffer.toByteArray()));
        valueBuffer.reset();
        
        context.write(null, m);
    }
    
    public static LongArrayWritable createSummary(Iterable<LongArrayWritable> flows, boolean includesLoaderPhase) throws IOException {
        log.info("in createSummary");
        ;
        TreeSet<Long> pDurations = new TreeSet<>();
        TreeSet<Long> iDelays = new TreeSet<>();
        TreeSet<Long> iDurations = new TreeSet<>();
        TreeSet<Long> lDelays = new TreeSet<>();
        TreeSet<Long> lDurations = new TreeSet<>();
        
        long eventCount = 0;
        for (LongArrayWritable flow_tmp : flows) {
            ArrayList<LongWritable> flow = flow_tmp.convert();
            int pos = 0;
            eventCount += flow.get(pos++).get();
            pDurations.add(flow.get(pos++).get());
            iDelays.add(flow.get(pos++).get());
            iDurations.add(flow.get(pos++).get());
            if (includesLoaderPhase) {
                lDelays.add(flow.get(pos++).get());
                lDurations.add(flow.get(pos++).get());
            }
        }
        
        {
            StringBuilder msg = new StringBuilder(256);
            msg.append("\nFlows:\n").append("\tRaw File Transform Durations: ").append(pDurations.size()).append('\n').append("\tIngest Delays: ")
                            .append(iDelays.size()).append('\n').append("\tIngest Durations: ").append(iDurations.size()).append('\n');
            if (includesLoaderPhase) {
                msg.append("\tLoader Delays: ").append(lDelays.size()).append('\n').append("\tLoader Durations: ").append(lDurations.size());
            }
            msg.append('\n');
            log.info(msg.toString());
        }
        
        LongWritable[] times = new LongWritable[includesLoaderPhase ? MetricsDataFormat.BULK_LENGTH : MetricsDataFormat.LIVE_LENGTH];
        int pos = 0;
        times[pos++] = new LongWritable(eventCount);
        times[pos++] = new LongWritable(findMedian(pDurations));
        times[pos++] = new LongWritable(findMedian(iDelays));
        times[pos++] = new LongWritable(findMedian(iDurations));
        if (includesLoaderPhase) {
            times[pos++] = new LongWritable(findMedian(lDelays));
            times[pos++] = new LongWritable(findMedian(lDurations));
        }
        
        LongArrayWritable metadata = new LongArrayWritable();
        metadata.set(times);
        
        return metadata;
    }
    
    /**
     * Utility method for finding the median in a sorted set.
     * 
     * @param s
     *            the set to search
     * @param <T>
     *            type of the set
     * @return the median value in the set or null if set is empty
     */
    public static <T> T findMedian(SortedSet<T> s) {
        if (s == null)
            return null;
        if (s.isEmpty())
            return null;
        
        int med = s.size() / 2;
        if (med == 0) {
            return s.first();
        }
        Iterator<T> itr = s.iterator();
        T t = null;
        int i = 0;
        while (itr.hasNext() && i < med) {
            t = itr.next();
            ++i;
        }
        return t;
    }
    
    /**
     * Utility method for getting the average of longs. Hopes to avoid overflow of adding longs together by doing the window method.
     * 
     * @param data
     *            collection of the longs to average
     * @return the average of all long values
     */
    public long getAverage(Collection<LongWritable> data) {
        int size = data.size();
        long average = 0l;
        for (LongWritable l : data) {
            average += l.get() / size;
        }
        return average;
    }
}
