package datawave.metrics.iterators;

import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import datawave.metrics.keys.IngestEntryKey;
import datawave.metrics.keys.InvalidKeyException;

/**
 * Modifies IngestEntryKeys so that a tuple of form [Timestamp][Rate] is returned.
 *
 */
public class IngestRateIterator extends RowIterator {
    static final Value ev = new Value(new byte[0]);
    static final Text et = new Text(new byte[0]);

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        IngestRateIterator iri = new IngestRateIterator();
        iri.setSource(getSource().deepCopy(env));
        return iri;
    }

    @Override
    public IteratorOptions describeOptions() {
        return null;
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        return true;
    }

    @Override
    protected void processRow(SortedMap<Key,Value> row) {
        LinkedList<Double> rates = new LinkedList<>();
        Text timestamp = row.firstKey().getRow();
        IngestEntryKey iek = new IngestEntryKey();
        for (Entry<Key,Value> e : row.entrySet()) {
            try {
                iek.parse(e.getKey());
            } catch (InvalidKeyException e1) {
                continue;
            }

            // value will be in Events/s
            double rate = ((double) iek.getCount()) / (((double) iek.getDuration()) / 1000.0);
            rates.add(rate);
        }

        // get the avg
        double avgRate = 0;
        for (Double d : rates) {
            avgRate += d / rates.size();
        }

        row.clear();
        row.put(new Key(timestamp, new Text(Double.toString(avgRate)), et), ev);
    }

}
