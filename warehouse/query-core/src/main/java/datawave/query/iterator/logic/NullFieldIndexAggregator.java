package datawave.query.iterator.logic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.jexl.functions.FieldIndexAggregator;

/**
 * A specialized {@link FieldIndexAggregator} that, instead of scanning all indexed terms for a document, returns a single key per document ID and immediately
 * seeks past the remaining terms.
 */
public class NullFieldIndexAggregator implements FieldIndexAggregator {

    private static final String MAX_BOUNDARY = "\0" + Character.MAX_VALUE;

    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr) throws IOException {
        return apply(itr, null, Collections.emptyList(), false);
    }

    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Range range, Collection<ByteSequence> columnFamilies, boolean includeColumnFamilies)
                    throws IOException {
        if (itr.hasTop()) {
            Key topKey = itr.getTopKey();

            // Get the current key's column qualifier.
            Text cq = new Text();
            topKey.getColumnQualifier(cq);

            // Append bytes to skip past this document entry.
            byte[] boundaryBytes = MAX_BOUNDARY.getBytes(StandardCharsets.UTF_8);
            cq.append(boundaryBytes, 0, boundaryBytes.length);

            // Build the target key to seek past.
            Key startKey = new Key(topKey.getRow(), topKey.getColumnFamily(), cq);
            Key endKey = (range != null) ? range.getEndKey() : null;
            boolean endKeyInclusive = (range != null) && range.isEndKeyInclusive();

            // Seek past the remaining terms.
            itr.seek(new Range(startKey, false, endKey, endKeyInclusive), columnFamilies, includeColumnFamilies);

            return topKey;
        }
        return null;
    }

    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Document doc, AttributeFactory attrs) throws IOException {
        return apply(itr, null, Collections.emptyList(), false);
    }
}
