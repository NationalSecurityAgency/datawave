package datawave.next.retrieval;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.data.parsers.EventKey;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.HitListArithmetic;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.Tuple3;

/**
 * An iterator that retrieves documents from the shard table.
 */
public class DocumentIterator extends DocumentIteratorOptions implements SortedKeyValueIterator<Key,Value> {

    private Key tk = null;
    private Value tv = null;

    private Range range = null;
    private Collection<ByteSequence> columnFamilies = null;
    private boolean inclusive = false;

    private final KryoDocumentSerializer serializer = new KryoDocumentSerializer();
    private final List<Entry<Key,Value>> results = new LinkedList<>();

    public DocumentIterator() {}

    public DocumentIterator(DocumentIterator other, IteratorEnvironment env) {
        this.source = other.source.deepCopy(env);
        super.deepCopy(other);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        this.options = options;
        this.env = env;
        validateOptions(this.options);
    }

    @Override
    public boolean hasTop() {
        if (tk == null) {
            if (!results.isEmpty()) {
                Entry<Key,Value> entry = results.remove(0);
                tk = entry.getKey();
                tv = entry.getValue();
            }
        }
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.range = range;
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;

        checkForScanRebuild();

        // aggregate document
        AttributeFactory attributeFactory = new AttributeFactory(typeMetadata);
        EventKey parser = new EventKey();

        for (String candidate : candidates) {

            Range candidateRange = rangeForCandidate(candidate);
            source.seek(candidateRange, excludeCFs, false);

            Document d = new Document();
            Key key = null;
            while (source.hasTop()) {
                key = source.getTopKey();
                source.next();

                if (timeFilter != null && !timeFilter.contains(key.getTimestamp())) {
                    // check for time stamp just in case
                    continue;
                }

                parser.parse(key);

                String field = JexlASTHelper.deconstructIdentifier(parser.getField());
                if (includeFields != null && !includeFields.contains(field)) {
                    // field was not present in inclusive filter
                    continue;
                } else if (excludeFields != null && excludeFields.contains(field)) {
                    // field matched the exclusive filter
                    continue;
                }

                Attribute<?> attr = attributeFactory.create(field, parser.getValue(), key, parser.getDatatype(), true);
                d.put(field, attr);
            }

            JexlEvaluation evaluation = new JexlEvaluation(query, new HitListArithmetic());

            ASTJexlScript queryTree = parse(query);
            Set<String> identifiers = JexlASTHelper.getIdentifierNames(queryTree);

            DatawaveJexlContext context = new DatawaveJexlContext();
            d.visit(identifiers, context);

            boolean matched = evaluation.apply(new Tuple3<>(tk, d, context));
            if (!matched) {
                continue;
            }

            if (d.size() > 0 && key != null) {
                Text cf = new Text(parser.getDatatype() + "\0" + parser.getUid());
                Key recordId = new Key(key.getRow(), cf, new Text(), key.getColumnVisibility(), key.getTimestamp());
                Attribute<?> attr = new DocumentKey(recordId, false);
                d.put(Document.DOCKEY_FIELD_NAME, attr);
            }

            if (d.size() > 0) {
                Map.Entry<Key,Document> entry = new AbstractMap.SimpleEntry<>(key, d);
                Map.Entry<Key,Value> result = serializer.apply(entry);
                results.add(result);
            }
        }
    }

    /**
     * Check if the start key is exclusive. If
     */
    private void checkForScanRebuild() {
        if (!range.isStartKeyInclusive()) {
            String cf = range.getStartKey().getColumnFamily().toString();
            if (cf != null && !cf.isEmpty() && cf.indexOf('\u0000') != -1) {
                // reasonably sure that if the column family is not empty and a null byte exists then
                // we can use this to pair down the candidate list
                while (!candidates.isEmpty() && candidates.get(0).compareTo(cf) <= 0) {
                    candidates.remove(0);
                }
            }
        }
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private Range rangeForCandidate(String candidate) {
        Key start = new Key(range.getStartKey().getRow(), new Text(candidate));
        Key stop = start.followingKey(PartialKey.ROW_COLFAM);
        return new Range(start, true, stop, false);
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
        Preconditions.checkNotNull(source, "deepCopy() called with null source");
        return source.deepCopy(env);
    }
}
