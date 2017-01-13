package nsa.datawave.metrics.web.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeSet;
import java.util.SortedSet;
import java.util.IdentityHashMap;
import java.util.Map;

import nsa.datawave.metrics.analytic.LongArrayWritable;
import nsa.datawave.metrics.analytic.MetricsCorrelatorReducer;
import nsa.datawave.metrics.analytic.MetricsDataFormat;
import nsa.datawave.metrics.analytic.MetricsDataFormat.MetricsField;

import org.apache.hadoop.io.LongWritable;

import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

public class MetricsSummary {
    private Multimap<MetricsField,LongWritable> liveData;
    private Multimap<MetricsField,LongWritable> bulkData;
    
    public MetricsSummary() {
        liveData = TreeMultimap.create();
        bulkData = TreeMultimap.create();
    }
    
    public int size() {
        return liveData.size() + bulkData.size();
    }
    
    public void addEntry(Value v) {
        LongArrayWritable array = new LongArrayWritable();
        try {
            array.readFields(new DataInputStream(new ByteArrayInputStream(v.get())));
            if (array.get().length == MetricsDataFormat.LIVE_LENGTH) {
                addEntry(array, MetricsDataFormat.LiveFields, liveData);
            } else {
                addEntry(array, MetricsDataFormat.BulkFields, bulkData);
            }
        } catch (IOException | IndexOutOfBoundsException e) {
            // don't add it
            // TODO log it
            
        }
    }
    
    private void addEntry(LongArrayWritable tuple, Collection<MetricsField> fields, Multimap<MetricsField,LongWritable> data) {
        ArrayList<LongWritable> _tuple = tuple.convert();
        for (MetricsField mf : fields) {
            data.put(mf, _tuple.get(mf.ordinal()));
        }
        data.put(MetricsField.EVENT_COUNT, _tuple.get(MetricsField.EVENT_COUNT.ordinal()));
    }
    
    private Map<MetricsField,Number> getSummary(Multimap<MetricsField,LongWritable> data, Collection<MetricsField> fields) {
        IdentityHashMap<MetricsField,Number> summary = new IdentityHashMap<>();
        for (MetricsField mf : fields) {
            SortedSet<LongWritable> ss = toSortedSet(data.get(mf));
            long median = MetricsCorrelatorReducer.findMedian(ss).get();
            summary.put(mf, median);
        }
        summary.put(MetricsField.EVENT_COUNT, getSum(data.get(MetricsField.EVENT_COUNT)));
        return summary;
    }
    
    public Map<MetricsField,Number> getLiveSummary() {
        return getSummary(liveData, MetricsDataFormat.LiveFields);
    }
    
    public Map<MetricsField,Number> getBulkSummary() {
        return getSummary(bulkData, MetricsDataFormat.BulkFields);
    }
    
    public static long getSum(Collection<LongWritable> vals) {
        long sum = 0;
        for (LongWritable lw : vals) {
            sum += lw.get();
        }
        return sum;
    }
    
    public static <T> SortedSet<T> toSortedSet(Collection<T> c) {
        TreeSet<T> s = new TreeSet<>();
        for (T t : c) {
            s.add(t);
        }
        return s;
    }
}
