package datawave.query.jexl.functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.tld.TLD;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import datawave.query.data.parsers.DatawaveKey;
import datawave.query.Constants;
import datawave.query.util.Tuple2;

/**
 * Aggregator for TF keys. TF keys that will be aggregated will be matching row and dataType/uid. FIELD/VALUE are not evaluated for performance reasons since
 * the likelyhood of a collision is extremely small
 */
public class TermFrequencyAggregator extends IdentityAggregator {

    public TermFrequencyAggregator(Set<String> fieldsToKeep, EventDataQueryFilter attrFilter, int maxNextCount) {
        super(fieldsToKeep, attrFilter, maxNextCount);
    }

    public TermFrequencyAggregator(Set<String> fieldsToKeep, EventDataQueryFilter attrFilter) {
        this(fieldsToKeep, attrFilter, -1);
    }

    @Override
    protected List<Tuple2<String,String>> parserFieldNameValue(Key topKey) {
        DatawaveKey parser = new DatawaveKey(topKey);
        return Arrays.asList(new Tuple2<>(parser.getFieldName(), parser.getFieldValue()));
    }

    @Override
    protected ByteSequence parseFieldNameValue(ByteSequence cf, ByteSequence cq) {
        return TLD.parseFieldAndValueFromTF(cq);
    }

    @Override
    protected ByteSequence parsePointer(ByteSequence qualifier) {
        ArrayList<Integer> deezNulls = TLD.instancesOf(0, qualifier, -1);
        final int stop = deezNulls.get(1);
        return qualifier.subSequence(0, stop);
    }

    @Override
    protected boolean samePointer(Text row, ByteSequence pointer, Key key) {
        if (row.equals(key.getRow())) {
            ByteSequence pointer2 = parsePointer(key.getColumnQualifierData());
            return (pointer.equals(pointer2));
        }
        return false;
    }

    @Override
    protected Key getSeekStartKey(Key current, ByteSequence pointer) {
        // CQ = dataType\0UID\0Normalized field value\0Field name
        // seek to the next documents TF
        return new Key(current.getRow(), current.getColumnFamily(), new Text(pointer + Constants.NULL_BYTE_STRING + Constants.MAX_UNICODE_STRING));
    }

    /**
     * Dropping empty documents is explicitly necessary for the negated leading wildcard case when dealing with a document specific range. This generates a scan
     * over all keys for that field on that document. Without this check if the aggregator runs, but finds no specific matches there will still be a docKey
     * generated and this will be returned back up through the chain resulting in a missed hit.
     *
     * Consider: '!(FIELD =~ '.*z)'
     *
     * If a document d has FIELD=b, this will in the TermFrequencyIndexIterator must scan against the entire range \0 to MAX for this document. This will cause
     * a document to be generated for our docKey b even though it doesn't match the entire regex (verified in the filter within the aggregation). If the
     * document were going all the way to evaluation this wouldn't matter, but it will be skipped when popping off the nestedIterators due to the way NOT is
     * short circuited. Since in this case the Range must be overly broad to prevent missing a match, we have to rely on the aggregators which employ the filter
     * logic to protect us.
     *
     * This check ensures that post aggregation (and application of the filter) there was actually something to get. Since the field is negated, the presence of
     * the docKey will cause the document to be excluded, even though it satisfies the query.
     *
     * An alternative to adding this check in the TermFrequencyAggregator would be to add this check to the TermFrequencyIndexIterator following the
     * aggregation.
     **/
    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Document doc, AttributeFactory attrs) throws IOException {
        Key key = super.apply(itr, doc, attrs);

        // only return a key if something was added to the document, documents that only contain Document.DOCKEY_FIELD_NAME as they found nothing of value to
        // aggregate
        if (doc.size() == 1 && doc.get(Document.DOCKEY_FIELD_NAME) != null) {
            key = null;

            // empty the document
            doc.remove(Document.DOCKEY_FIELD_NAME);
        }

        return key;
    }
}
