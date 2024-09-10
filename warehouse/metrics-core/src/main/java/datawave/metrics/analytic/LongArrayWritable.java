package datawave.metrics.analytic;

import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class LongArrayWritable extends ArrayWritable {
    public LongArrayWritable() {
        super(LongWritable.class);
    }

    public ArrayList<LongWritable> convert() {
        Writable[] backingStore = get();
        ArrayList<LongWritable> array = new ArrayList<>(backingStore.length);
        for (Writable w : get()) {
            array.add((LongWritable) w);
        }
        return array;
    }
}
