package datawave.query.jexl.functions;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.data.parsers.FieldIndexKey;
import datawave.query.data.parsers.KeyParser;
import datawave.query.jexl.JexlASTHelper;

/**
 * A field index aggregator for TLD queries
 */
public class TLDFiAggregator extends FiAggregator {

    public TLDFiAggregator() {
        // empty constructor
    }

    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Document d, AttributeFactory af) throws IOException {
        Key key = itr.getTopKey();
        tkParser.parse(key);
        Key k = getParentKey(tkParser);

        Key tk = key;
        while (tk != null) {
            parser.parse(tk);

            if (!sameUid(tkParser, parser)) {
                break;
            }

            Attribute<?> attr = af.create(parser.getField(), parser.getValue(), tk, true);
            // in addition to keeping fields that the filter indicates should be kept, also keep fields that the filter applies. This is due to inconsistent
            // behavior between event/tld queries where an index only field index will be kept except when it is a child of a tld

            boolean fieldKeep = fieldsToKeep == null || fieldsToKeep.contains(JexlASTHelper.removeGroupingContext(parser.getField()));
            boolean filterKeep = filter == null || filter.keep(tk);
            boolean toKeep = fieldKeep && filterKeep;
            attr.setToKeep(toKeep);
            d.put(parser.getField(), attr);

            // if the child id does not equal the parent id, add a new document key
            if (!tkParser.getUid().equals(parser.getUid()) || parser.getRootUid().equals(parser.getUid())) {
                d.put(Document.DOCKEY_FIELD_NAME, getRecordId(parser));
            }

            if (range != null && fieldMetadata != null && !fieldMetadata.isFieldContentFunction(parser.getField())) {
                // for non-event fields that are not part of a content function, perform a seeking aggregation
                Key startKey = getNextParentKey(tk, parser);
                Range seekRange = new Range(startKey, false, range.getEndKey(), true);
                itr.seek(seekRange, columnFamilies, true);
            } else {
                itr.next();
            }
            tk = (itr.hasTop() ? itr.getTopKey() : null);
        }
        return k;
    }

    // helper methods

    @Override
    protected Key getParentKey(KeyParser parser) {
        Text row = parser.getKey().getRow();
        Text cf = new Text(parser.getDatatype() + '\u0000' + parser.getRootUid());
        Text cq = new Text(parser.getField() + '\u0000' + parser.getValue());
        return new Key(row, cf, cq, parser.getKey().getColumnVisibility(), parser.getKey().getTimestamp());
    }

    /**
     * This override handles the path in {@link FiAggregator#apply(SortedKeyValueIterator)}
     *
     * @param parser
     *            a key parser
     * @param other
     *            a different key parser
     * @return true if the two uids are considered equivalent
     */
    @Override
    protected boolean sameUid(KeyParser parser, KeyParser other) {
        return parser.getRootUid().equals(other.getRootUid());
    }

    /**
     * Builds a seek range for the next TLD. This method handles the path at {@link FiAggregator#apply(SortedKeyValueIterator, Range, Collection, boolean)}
     *
     * @param parser
     *            a field index key parser
     * @param range
     *            a seek range
     * @return a seek range built for the next TLD
     */
    @Override
    protected Range getSeekRange(FieldIndexKey parser, Range range) {
        Key k = parser.getKey();
        Text cq = new Text(parser.getValue() + '\u0000' + parser.getDatatype() + '\u0000' + parser.getRootUid() + Constants.MAX_UNICODE_STRING);
        Key startKey = new Key(k.getRow(), k.getColumnFamily(), cq, 0);
        return new Range(startKey, false, range.getEndKey(), range.isEndKeyInclusive());
    }
}
