package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.IdentityAggregator;

/**
 * Iterate over a document range aggregating fields in their normalized form.
 */
public class EventFieldIterator implements NestedIterator<Key> {
    private final Range range;
    private final SortedKeyValueIterator<Key,Value> source;
    private final String field;
    private final IdentityAggregator aggregator;
    private final AttributeFactory attributeFactory;
    private Key key;
    private Document document;
    private boolean initialized = false;

    public EventFieldIterator(Range range, SortedKeyValueIterator<Key,Value> source, String field, AttributeFactory attributeFactory,
                    IdentityAggregator aggregator) {
        this.range = range;
        this.source = source;
        this.field = field;
        this.attributeFactory = attributeFactory;
        this.aggregator = aggregator;
    }

    @Override
    public void initialize() {
        // no-op
    }

    @Override
    public Key move(Key minimum) {
        // simple sanity check that is free
        if (!range.contains(minimum)) {
            return null;
        }

        // test current source key to determine state
        if (!source.hasTop()) {
            // no key can match the underlying source is empty
            return null;
        }

        Key top = source.getTopKey();

        if (minimum.compareTo(top) < 0) {
            throw new IllegalStateException("cannot move iterator backwards to " + minimum);
        }

        // update the range to start a minimum
        Range newRange = new Range(minimum, true, range.getEndKey(), range.isEndKeyInclusive());

        try {
            if (!initialized) {
                init(newRange);
            } else {
                // just seek
                source.seek(newRange, Collections.emptyList(), false);
                getNext();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        source.seek(range, columnFamilies, inclusive);
    }

    @Override
    public Collection<NestedIterator<Key>> leaves() {
        return Collections.emptySet();
    }

    @Override
    public Collection<NestedIterator<Key>> children() {
        return Collections.emptySet();
    }

    @Override
    public Document document() {
        return document;
    }

    @Override
    public boolean isContextRequired() {
        return false;
    }

    @Override
    public void setContext(Key context) {
        // no-op
    }

    @Override
    public boolean isNonEventField() {
        return false;
    }

    @Override
    public boolean hasNext() {
        // do the actual seeking now if it hasn't been done yet
        try {
            if (!initialized) {
                init(range);
            } else if (key == null && source.hasTop()) {
                getNext();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return key != null;
    }

    @Override
    public Key next() {
        Key toReturn = key;
        key = null;

        return toReturn;
    }

    private void init(Range seekRange) throws IOException {
        if (!initialized) {
            source.seek(seekRange, Collections.emptyList(), false);
            getNext();

            initialized = true;
        }
    }

    private void getNext() throws IOException {
        key = null;
        // normalizingIterator is already reduced down to the proper field and range, just aggregate if it has anything
        while (source.hasTop() && key == null) {
            Key topKey = source.getTopKey();
            String cq = topKey.getColumnQualifier().toString();
            int nullIndex1 = cq.indexOf('\u0000');

            if (nullIndex1 == -1) {
                // invalid key, keep going
                source.next();
                continue;
            }

            String fieldName = cq.substring(0, nullIndex1);

            if (JexlASTHelper.removeGroupingContext(fieldName).equals(field)) {
                document = new Document();
                key = aggregator.apply(source, document, attributeFactory);

                // only return a key if something was added to the document, documents that only contain Document.DOCKEY_FIELD_NAME
                // should not be returned
                if (document.size() == 1 && document.get(Document.DOCKEY_FIELD_NAME) != null) {
                    key = null;

                    // empty the document
                    document.remove(Document.DOCKEY_FIELD_NAME);
                }
            }

            source.next();
        }

        // clear the document if no next was found
        if (key == null) {
            document = null;
        }
    }
}
