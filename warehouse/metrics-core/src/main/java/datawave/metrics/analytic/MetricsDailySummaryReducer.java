package datawave.metrics.analytic;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Longs;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.hadoop.mapreduce.AccumuloOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.aggregate.LongValueSum;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Writes daily/hourly summary metrics information out to Accumulo. All incoming keys having the same date, metric name, metric value have their counts added
 * together before the ouptut is returned.
 */
public class MetricsDailySummaryReducer extends Reducer<Key,Value,Text,Mutation> {
    public static final String STATS_METRIC_VALUE = "__stats__";
    private static final Text STATS_METRIC_TEXT = new Text(STATS_METRIC_VALUE);
    public static final String PERCENTILE_STATS_METRIC_VALUE = "__pstats__";
    private static final Text PERCENTILE_STATS_METRIC_TEXT = new Text(PERCENTILE_STATS_METRIC_VALUE);
    private static final Text MIN_TEXT = new Text("MIN");
    private static final Text MAX_TEXT = new Text("MAX");
    private static final Text MEDIAN_TEXT = new Text("MEDIAN");
    private static final Text AVERAGE_TEXT = new Text("AVERAGE");
    private static final Text PERCENTILE_TEXT = new Text("95TH_PERCENTILE");
    private static final int MAX_MEDIAN_COUNT = 4000000;

    private Text holder = new Text();
    private ArrayList<Long> longs = new ArrayList<>(MAX_MEDIAN_COUNT);
    private ArrayList<WeightedPair> pairs = new ArrayList<>(MAX_MEDIAN_COUNT);
    private Text empty = new Text();

    @Override
    protected void reduce(Key key, Iterable<Value> values, Context context) throws IOException, InterruptedException {
        key.getColumnQualifier(holder);
        Mutation m;
        if (STATS_METRIC_TEXT.equals(holder))
            m = statsMutation(key, values);
        else if (PERCENTILE_STATS_METRIC_TEXT.equals(holder))
            m = statsPercentileMutation(key, values);
        else
            m = sumMutation(key, values);
        context.write(empty, m);
    }

    /**
     * Computes a simple summation metric value. The key is written out as is and the values, assumed to be string longs, are aggregated and written out.
     *
     * @param key
     *            a key
     * @param values
     *            list of values
     * @return an output mutation
     */
    private Mutation sumMutation(Key key, Iterable<Value> values) {
        LongValueSum sum = new LongValueSum();
        for (Value v : values)
            sum.addNextValue(v);

        ColumnVisibility columnVisibility = new ColumnVisibility(key.getColumnVisibility());
        Mutation m = new Mutation(key.getRow());
        m.put(key.getColumnFamily(), key.getColumnQualifier(), columnVisibility, new Value(sum.getReport().getBytes()));
        return m;
    }

    /**
     * Computes a "stats" metric value. Each incoming value in {@code values} is assumed to be a unique value. We calculate min, max, median, and average values
     * and include those in the output mutation. Note that median will only be calculated if there are not more than {@code MAX_MEDIAN_COUNT} values. This
     * restriction prevents us from using too much memory in the task tracker.
     *
     * @param key
     *            a key
     * @param values
     *            a list of values
     * @return an output mutation
     */
    private Mutation statsMutation(Key key, Iterable<Value> values) {
        long numLongs = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        longs.clear();
        LongValueSum sum = new LongValueSum();
        for (Value v : values) {
            ++numLongs;
            long aLong = Long.parseLong(v.toString());
            min = Math.min(aLong, min);
            max = Math.max(aLong, max);
            sum.addNextValue(aLong);

            if (numLongs <= MAX_MEDIAN_COUNT)
                longs.add(aLong);
        }
        numLongs = Math.max(1, numLongs);

        String average = String.format("%1.4f", sum.getSum() / (double) numLongs);

        ColumnVisibility columnVisibility = new ColumnVisibility(key.getColumnVisibility());
        Text columnFamily = key.getColumnFamily();
        Mutation m = new Mutation(key.getRow());
        m.put(columnFamily, MIN_TEXT, columnVisibility, new Value(Long.toString(min).getBytes()));
        m.put(columnFamily, MAX_TEXT, columnVisibility, new Value(Long.toString(max).getBytes()));
        m.put(columnFamily, AVERAGE_TEXT, columnVisibility, new Value(average.getBytes()));
        if (numLongs <= MAX_MEDIAN_COUNT) {
            Collections.sort(longs);
            String median = "" + longs.get(longs.size() / 2);
            m.put(columnFamily, MEDIAN_TEXT, columnVisibility, new Value(median.getBytes()));
        }
        return m;
    }

    /**
     * Computes a "percentile stats" metric value. Each incoming value in {@code values} is assumed to be a unique value. We calculate min, max, median, and
     * average values and include those in the output mutation. Note that median will only be calculated if there are not more than {@code MAX_MEDIAN_COUNT}
     * values. This restriction prevents us from using too much memory in the task tracker. In this variant, each value is assumed to be a serialized
     * {@link WeightedPair} object. If the median can be calculated, then the 95% percentile weighted value is also emitted as a statistic. The weights are
     * summed, and the value at the 95th percentile of the weights is returned. (e.g., for event latencies, each pair's value is the latency and the weight is
     * the number of events at that latency, so we return the latency (value) from the list that represents 95% of the events (weights).
     *
     * @param key
     *            a key
     * @param values
     *            a list of values
     * @return the output mutation
     */
    private Mutation statsPercentileMutation(Key key, Iterable<Value> values) {
        long numPairs = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        pairs.clear();
        LongValueSum valueSum = new LongValueSum();
        LongValueSum weightSum = new LongValueSum();
        for (Value v : values) {
            ++numPairs;
            WeightedPair pair = WeightedPair.parseValue(v);
            min = Math.min(pair.getValue(), min);
            max = Math.max(pair.getValue(), max);
            valueSum.addNextValue(pair.getValue());
            weightSum.addNextValue(pair.getWeight());

            if (numPairs <= MAX_MEDIAN_COUNT)
                pairs.add(pair);
        }
        numPairs = Math.max(1, numPairs);

        String average = String.format("%1.4f", valueSum.getSum() / (double) numPairs);

        ColumnVisibility columnVisibility = new ColumnVisibility(key.getColumnVisibility());
        Text columnFamily = key.getColumnFamily();
        Mutation m = new Mutation(key.getRow());
        m.put(columnFamily, MIN_TEXT, columnVisibility, new Value(Long.toString(min).getBytes()));
        m.put(columnFamily, MAX_TEXT, columnVisibility, new Value(Long.toString(max).getBytes()));
        m.put(columnFamily, AVERAGE_TEXT, columnVisibility, new Value(average.getBytes()));
        if (numPairs <= MAX_MEDIAN_COUNT) {
            Collections.sort(pairs);
            String median = "" + pairs.get(pairs.size() / 2).getValue();
            m.put(columnFamily, MEDIAN_TEXT, columnVisibility, new Value(median.getBytes()));

            // Figure out the position in the list where the sum of the weights up to that point is
            // at the 95% percentile. We'll then take the value at that position. Go through the
            // list backwards in hope that the weights are mostly even and therefore the 95% weight
            // will be somewhere around 95% of the way through the list.
            long percentileWeight = (long) Math.ceil(weightSum.getSum() * 0.95);
            long positionWeight = weightSum.getSum();
            for (int i = pairs.size() - 1; i >= 0; --i) {
                WeightedPair pair = pairs.get(i);
                positionWeight -= pair.getWeight();
                if (positionWeight < percentileWeight) { // this pair's weight contribution took us to or over the 95th percentile weight...
                    String percentile = "" + pair.getValue();
                    m.put(columnFamily, PERCENTILE_TEXT, columnVisibility, new Value(percentile.getBytes()));
                    break;
                }
            }
        }
        return m;
    }

    public static void configureJob(Job job, int numDays, String instance, String zookeepers, String userName, String password, String outputTable) {
        job.setNumReduceTasks(Math.min(numDays, 100)); // Cap the number of reducers at 100, just in case we have a large day range (shouldn't really happen
                                                       // though)
        job.setReducerClass(MetricsDailySummaryReducer.class);
        job.setOutputFormatClass(AccumuloOutputFormat.class);
        // @formatter:off
        AccumuloOutputFormat.configure()
                .clientProperties(Accumulo.newClientProperties().to(instance,zookeepers).as(userName, password).build())
                .createTables(true)
                .defaultTable(outputTable)
                .store(job);
        // @formatter:on
    }

    public static class WeightedPair implements Writable, Comparable<WeightedPair> {
        private long value;
        private long weight;

        public WeightedPair() {}

        public WeightedPair(long value, long weight) {
            this.value = value;
            this.weight = weight;
        }

        public long getValue() {
            return value;
        }

        public long getWeight() {
            return weight;
        }

        public Value toValue() {
            ByteArrayDataOutput out = ByteStreams.newDataOutput();
            try {
                write(out);
            } catch (IOException e) {
                // can't happen - it's a byte array stream
            }
            return new Value(out.toByteArray());
        }

        @Override
        public int compareTo(WeightedPair o) {
            return Longs.compare(value, o.value);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(value);
            out.writeLong(weight);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            value = in.readLong();
            weight = in.readLong();
        }

        public static WeightedPair parseValue(Value value) {
            WeightedPair p = new WeightedPair();
            try {
                p.readFields(ByteStreams.newDataInput(value.get()));
            } catch (IOException e) {
                // can't happen - it's a byte array stream
            }
            return p;
        }
    }
}
