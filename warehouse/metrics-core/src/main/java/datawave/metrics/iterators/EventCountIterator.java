package datawave.metrics.iterators;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import datawave.metrics.keys.IngestEntryKey;
import datawave.metrics.keys.InvalidKeyException;
import datawave.metrics.util.WritableUtil;

import org.apache.hadoop.io.Text;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Go over the metrics timeseries and pull out event counts.
 *
 */
public class EventCountIterator extends RowIterator {
    private static final Map<String,String> esm = new TreeMap<>();
    private static final List<String> esl = new LinkedList<>();
    private static final IteratorOptions OPTIONS = new IteratorOptions("EventCountIterator", "Sums event counts in an IngestEntryKey", esm, esl);

    private static final Value empty = new Value(new byte[0]);

    @Override
    public IteratorOptions describeOptions() {
        return OPTIONS;
    }

    @Override
    protected void processRow(SortedMap<Key,Value> row) {
        Text timestamp = row.firstKey().getRow();
        long count = 0;
        TreeSet<String> jobIds = new TreeSet<>();
        for (Key k : row.keySet()) {
            IngestEntryKey iek;
            try {
                iek = new IngestEntryKey(k);
            } catch (InvalidKeyException e) {
                continue;
            }
            count += iek.getCount();
            jobIds.add(iek.getJobId());
        }
        row.clear();
        Key newKey = new Key(timestamp, new Text(Long.toString(count)), WritableUtil.EmptyText);
        row.put(newKey, new Value(collectionToCsv(jobIds).getBytes()));
    }

    private String collectionToCsv(Collection<String> cs) {
        StringBuilder sb = new StringBuilder();
        for (String s : cs) {
            sb.append(s).append(',');
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        EventCountIterator eci = new EventCountIterator();
        eci.setSource(this.getSource().deepCopy(env));
        return eci;
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        return true;
    }

    @Override
    public Value getTopValue() {
        return empty;
    }

}
