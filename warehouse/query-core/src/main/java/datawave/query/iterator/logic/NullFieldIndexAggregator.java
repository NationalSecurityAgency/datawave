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
 * A specialized {@link FieldIndexAggregator} that handles null and not-null checks efficiently for non-event (index-only) fields.
 * <p>
 * Instead of scanning all indexed terms for a document, it returns a single key per document ID and immediately seeks past the remaining terms.
 */
public class NullFieldIndexAggregator implements FieldIndexAggregator {

    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr) throws IOException {
        return apply(itr, null, Collections.emptyList(), false);
    }

    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Range range, Collection<ByteSequence> columnFamilies, boolean includeColumnFamilies)
                    throws IOException {
        if (itr.hasTop()) {
            Key topKey = itr.getTopKey();

            // Get the current key's Column Qualifier
            Text cq = new Text();
            topKey.getColumnQualifier(cq);

            // Append bytes to skip past this document entry
            byte[] boundaryBytes = "\0\uFFFF".getBytes(StandardCharsets.UTF_8);
            cq.append(boundaryBytes, 0, boundaryBytes.length);

            // Build target key
            Key jumpTargetKey = new Key(topKey.getRow(), topKey.getColumnFamily(), cq);

            Key endKey = (range != null) ? range.getEndKey() : null;
            boolean endKeyInclusive = (range != null) && range.isEndKeyInclusive();

            // Create range to jump past jumpTargetKey
            Range jumpRange = new Range(jumpTargetKey, false, endKey, endKeyInclusive);

            itr.seek(jumpRange, columnFamilies, includeColumnFamilies);

            return topKey;
        }
        return null;
    }

    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Document doc, AttributeFactory attrs) throws IOException {
        return apply(itr, null, Collections.emptyList(), false);
    }
}
