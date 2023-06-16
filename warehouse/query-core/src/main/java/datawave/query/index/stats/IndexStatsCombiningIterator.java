package datawave.query.index.stats;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

/**
 * Sum each column in the tuple and divide the the sum of the unique values by the sum of the total count values and return a relative weight.
 *
 */
public class IndexStatsCombiningIterator implements SortedKeyValueIterator<Key,Value> {

    private SortedKeyValueIterator<Key,Value> src;
    private Key tk = null;
    private Value tv = null;
    private final DoubleWritable weight = new DoubleWritable();

    // optmization
    private final IndexStatsRecord tuple = new IndexStatsRecord();

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        src = source;
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        if (src.hasTop()) {
            Key srcTK = src.getTopKey();
            Text workingRow = srcTK.getRow();
            Text currentRow = srcTK.getRow();

            long sumUnique = 0;
            long sumCount = 0;

            while (workingRow.equals(currentRow)) {
                tuple.readFields(new DataInputStream(new ByteArrayInputStream(src.getTopValue().get())));
                sumUnique += tuple.getNumberOfUniqueWords().get();
                sumCount += tuple.getWordCount().get();

                src.next();
                if (src.hasTop()) {
                    srcTK = src.getTopKey();
                    srcTK.getRow(currentRow);
                } else {
                    break;
                }
            }
            weight.set(((double) sumUnique) / ((double) sumCount));
            tk = new Key(workingRow);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            weight.write(new DataOutputStream(baos));
            tv = new Value(baos.toByteArray());
        } else {
            tk = null;
            tv = null;
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        src.seek(range, columnFamilies, inclusive);
        next();
    }

    @Override
    public Key getTopKey() {
        return tk;
    }

    @Override
    public Value getTopValue() {
        return tv;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        IndexStatsCombiningIterator isci = new IndexStatsCombiningIterator();
        isci.src = src.deepCopy(env);
        return isci;
    }

}
