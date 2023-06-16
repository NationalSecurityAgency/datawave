package datawave.metrics.iterators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import datawave.metrics.analytic.LongArrayWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

/**
 * Scans over the DatawaveMetrics table and sums up all the fields in the value aside from the first element, which is an event count.
 *
 */
public class LatencyIterator extends WrappingIterator {

    public LatencyIterator() {

    }

    public LatencyIterator(LatencyIterator other, IteratorEnvironment env) {
        setSource(other.getSource().deepCopy(env));
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new LatencyIterator(this, env);
    }

    // @Override
    // public Key getTopKey() {
    // return new Key(getSource().getTopKey().getRow());
    // }

    @Override
    public Value getTopValue() {
        LongArrayWritable typeInfo = new LongArrayWritable();
        try {
            typeInfo.readFields(new DataInputStream(new ByteArrayInputStream(getSource().getTopValue().get())));

            List<LongWritable> numbers = typeInfo.convert();
            long sum = 0;
            for (int i = 1; i < numbers.size(); ++i) {
                sum += numbers.get(i).get();
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            new LongWritable(sum).write(new DataOutputStream(baos));
            return new Value(baos.toByteArray());
        } catch (IOException e) {
            return new Value(new byte[0]);
        }
    }
}
