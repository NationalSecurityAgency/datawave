package datawave.metrics.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

/**
 * A Map task that imports bulk loader metrics into Accumulo. The inputs are metrics files from MapFileLoader and it places the metrics into an accumulo table
 * with the structure:
 * <p>
 *
 * <pre>
 *  Row             Colf            Colq               Value
 * ---------------------------------------------------------
 * |end time        |directory      |start time        | job counters file
 * </pre>
 * <p>
 *
 * Because there is no actual tag for the data, there does not need to be a Structure convenience class that standardizes the strings used to organize data.
 *
 */
public class LoaderMetricsMapper extends Mapper<NullWritable,Counters,Text,Mutation> {

    static final byte[] EMPTY_BYTES = new byte[0];
    static final Value EMPTY_VALUE = new Value(EMPTY_BYTES);

    private enum counters {
        DIRECTORIES_STARTED, DIRECTORIES_ENDED
    }

    public void map(NullWritable k, Counters v, Context context) throws IOException, InterruptedException {

        CounterGroup startGroup = v.getGroup("MapFileLoader.StartTimes");
        TreeMap<String,Long> directoryStartTimes = new TreeMap<>();
        for (Counter c : startGroup) {
            directoryStartTimes.put(c.getName(), c.getValue());
            context.getCounter(counters.DIRECTORIES_STARTED).increment(1);
        }

        CounterGroup endGroup = v.getGroup("MapFileLoader.EndTimes");
        TreeMap<String,Long> directoryEndTimes = new TreeMap<>();
        for (Counter c : endGroup) {
            directoryEndTimes.put(c.getName(), c.getValue());
            context.getCounter(counters.DIRECTORIES_ENDED).increment(1);
        }

        for (Entry<String,Long> endTime : directoryEndTimes.entrySet()) {
            String directory = endTime.getKey();
            long end = endTime.getValue();
            Long start = directoryStartTimes.get(directory);
            if (start != null) {
                Mutation m = new Mutation(new Text(directory));
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                v.write(new DataOutputStream(baos));
                m.put(new Text(Long.toString(end - start)), new Text(Long.toString(end)), new Value(baos.toByteArray()));
                context.write(null, m);
            }
        }
    }

}
