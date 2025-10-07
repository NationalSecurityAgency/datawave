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
import org.apache.hadoop.io.Text;

public class IndexStatsSummingIterator implements SortedKeyValueIterator<Key,Value> {
    private SortedKeyValueIterator<Key,Value> src;
    private Key tk = null;
    private Value tv = null;

    // optmization
    private final IndexStatsRecord tuple = new IndexStatsRecord();
    private final IndexStatsRecord summedValues = new IndexStatsRecord();

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
            summedValues.setNumberOfUniqueWords(sumUnique);
            summedValues.setWordCount(sumCount);
            tk = new Key(workingRow);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            summedValues.write(new DataOutputStream(baos));
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
        IndexStatsSummingIterator isci = new IndexStatsSummingIterator();
        isci.src = src.deepCopy(env);
        return isci;
    }
}
