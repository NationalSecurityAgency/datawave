package datawave.query.jexl.functions;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import datawave.marking.ColumnVisibilityCache;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.data.parsers.FieldIndexKey;
import datawave.query.data.parsers.KeyParser;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.QueryFieldMetadataVisitor.FieldMetadata;
import datawave.query.predicate.EventDataQueryFilter;

/**
 * A field index aggregator for event queries
 */
public class FiAggregator implements FieldIndexAggregator {

    private static final Text EMPTY_TEXT = new Text();

    private int maxNextCount = -1;
    protected Set<String> fieldsToKeep;
    protected EventDataQueryFilter filter;
    protected FieldMetadata fieldMetadata;

    protected final FieldIndexKey parser = new FieldIndexKey();
    protected final FieldIndexKey tkParser = new FieldIndexKey();

    public FiAggregator() {
        // empty constructor
    }

    // builder style methods for legacy values

    public FiAggregator withMaxNextCount(int maxNextCount) {
        this.maxNextCount = maxNextCount;
        return this;
    }

    public FiAggregator withFieldsToKeep(Set<String> fieldsToKeep) {
        this.fieldsToKeep = fieldsToKeep;
        return this;
    }

    public FiAggregator withQueryFilter(EventDataQueryFilter filter) {
        this.filter = filter;
        return this;
    }

    public FiAggregator withFieldMetadata(FieldMetadata fieldMetadata) {
        this.fieldMetadata = fieldMetadata;
        return this;
    }

    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr) throws IOException {
        Key key = itr.getTopKey();
        tkParser.parse(key);
        Key k = getParentKey(tkParser);

        while (itr.hasTop() && samePointer(tkParser, itr.getTopKey())) {
            itr.next();
        }

        return k;
    }

    /**
     * Advances the source iterator, using the provided range and column families
     *
     * @param iter
     *            the iterator
     * @param range
     *            current range
     * @param cfs
     *            the column families
     * @param includeColumnFamilies
     *            flag for including families
     * @return the document's record id
     * @throws IOException
     *             if something goes wrong
     */
    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> iter, Range range, Collection<ByteSequence> cfs, boolean includeColumnFamilies) throws IOException {
        Key tk = iter.getTopKey();
        tkParser.parse(tk);
        Key result = getParentKey(tkParser);

        int nextCount = 0;
        Key current;
        while (iter.hasTop()) {

            current = iter.getTopKey();
            parser.parse(current);

            if (!samePointer(tkParser, parser)) {
                break;
            }

            if (maxNextCount == -1 || nextCount < maxNextCount) {
                iter.next();
                nextCount++;
            } else {
                Range seekRange = getSeekRange(parser, range);
                iter.seek(seekRange, cfs, includeColumnFamilies);
                nextCount = 0;
            }
        }

        return result;
    }

    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Document doc, AttributeFactory attrs) throws IOException {
        Key key = itr.getTopKey();
        tkParser.parse(key);
        Key k = getParentKey(tkParser);

        Key tk = key;
        while (tk != null) {

            parser.parse(tk);

            if (!samePointer(tkParser, parser)) {
                break;
            }

            // in this case we only have a single field-value per key.
            // normalization introduces multiple versions

            Attribute<?> attr = attrs.create(parser.getField(), parser.getValue(), tk, true);
            // only keep fields that are index only and pass the attribute filter
            boolean fieldKeep = fieldsToKeep == null || fieldsToKeep.contains(JexlASTHelper.removeGroupingContext(parser.getField()));
            boolean filterKeep = filter == null || filter.keep(tk);
            boolean toKeep = fieldKeep && filterKeep;
            attr.setToKeep(toKeep);

            // This could be broken out into different cases.
            // EQ implies always keep
            // RE implies always evaluate
            // Anything that is being kept has to be added to the doc to be returned, if we aren't keeping only add to the doc if necessary for evaluation
            if (toKeep || (filter == null || filter.apply(new AbstractMap.SimpleEntry<>(tk, null)))) {
                doc.put(parser.getField(), attr);
            }

            itr.next();
            tk = (itr.hasTop() ? itr.getTopKey() : null);
        }

        doc.put(Document.DOCKEY_FIELD_NAME, getRecordId(tkParser));

        return k;
    }

    // helper methods

    protected Key getParentKey(KeyParser parser) {
        Text row = parser.getKey().getRow();
        Text cf = new Text(parser.getDatatype() + '\u0000' + parser.getUid());
        Text cq = new Text(parser.getField() + '\u0000' + parser.getValue());
        return new Key(row, cf, cq, parser.getKey().getColumnVisibility(), parser.getKey().getTimestamp());
    }

    protected boolean samePointer(FieldIndexKey tkParser, Key topKey) {
        parser.parse(topKey);
        return samePointer(tkParser, parser);
    }

    protected boolean samePointer(FieldIndexKey topKey, FieldIndexKey other) {
        //  @formatter:off
        return sameRow(topKey, other) &&
                        sameDatatype(topKey, other) &&
                        sameUid(topKey, other);
        //  @formatter:on
    }

    protected boolean sameRow(KeyParser parser, KeyParser other) {
        return parser.getKey().getRow().equals(other.getKey().getRow());
    }

    protected boolean sameDatatype(KeyParser parser, KeyParser other) {
        return parser.getDatatype().equals(other.getDatatype());
    }

    protected boolean sameUid(KeyParser parser, KeyParser other) {
        return parser.getUid().equals(other.getUid());
    }

    protected Range getSeekRange(FieldIndexKey parser, Range range) {
        Key k = parser.getKey();
        Key startKey = new Key(k.getRow(), k.getColumnFamily(), k.getColumnQualifier(), 0);
        return new Range(startKey, false, range.getEndKey(), range.isEndKeyInclusive());
    }

    protected DocumentKey getRecordId(KeyParser parser) {
        Text cf = new Text(parser.getDatatype() + '\u0000' + parser.getUid());
        ColumnVisibility cv = ColumnVisibilityCache.get(parser.getKey().getColumnVisibilityData());
        long ts = parser.getKey().getTimestamp();
        Key key = new Key(parser.getKey().getRow(), cf, EMPTY_TEXT, cv, ts);
        return new DocumentKey(key, false);
    }
}
