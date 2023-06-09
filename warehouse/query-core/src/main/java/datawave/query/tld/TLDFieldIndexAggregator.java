package datawave.query.tld;

import static datawave.query.tld.TLD.parsePointerFromFI;
import static datawave.query.tld.TLD.parseRootPointerFromFI;

import java.io.IOException;

import java.util.Set;

import datawave.marking.ColumnVisibilityCache;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.FieldIndexAggregator;

import datawave.query.jexl.functions.SeekingAggregator;
import datawave.query.predicate.EventDataQueryFilter;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

public class TLDFieldIndexAggregator extends SeekingAggregator implements FieldIndexAggregator {
    private Set<String> fieldsToAggregate;
    private EventDataQueryFilter attrFilter;

    public TLDFieldIndexAggregator(Set<String> fieldsToAggregate, EventDataQueryFilter attrFilter) {
        this(fieldsToAggregate, attrFilter, -1);
    }

    public TLDFieldIndexAggregator(Set<String> fieldsToAggregate, EventDataQueryFilter attrFilter, int maxNextCount) {
        super(maxNextCount);
        this.fieldsToAggregate = fieldsToAggregate;
        this.attrFilter = attrFilter;
    }

    public Key apply(SortedKeyValueIterator<Key,Value> itr, Document d, AttributeFactory af) throws IOException {
        Key key = itr.getTopKey();
        ByteSequence parentId = parseRootPointerFromFI(key.getColumnQualifierData());
        Text row = key.getRow();
        ByteSequence docId = null;
        Key nextKey = key;
        do {
            key = nextKey;
            String field = key.getColumnFamily().toString().substring(3);
            String value = key.getColumnQualifier().toString();
            value = value.substring(0, value.indexOf('\0'));
            Attribute<?> attr = af.create(field, value, key, true);
            // in addition to keeping fields that the filter indicates should be kept, also keep fields that the filter applies. This is due to inconsistent
            // behavior between event/tld queries where an index only field index will be kept except when it is a child of a tld
            attr.setToKeep((fieldsToAggregate == null || fieldsToAggregate.contains(JexlASTHelper.removeGroupingContext(field)))
                            && (attrFilter == null || attrFilter.keep(key)));
            d.put(field, attr);

            ByteSequence thisId = parsePointerFromFI(key.getColumnQualifierData());
            if (docId == null || !docId.equals(thisId)) {
                docId = thisId;
                Key docKey = new Key(key.getRow(), new Text(docId.toArray()), new Text(), ColumnVisibilityCache.get(key.getColumnVisibilityData()),
                                key.getTimestamp());
                attr = new DocumentKey(docKey, false);
                d.put(Document.DOCKEY_FIELD_NAME, attr);
            }
            itr.next();
            nextKey = itr.hasTop() ? itr.getTopKey() : null;
        } while (skip(nextKey, row, parentId));
        return getResult(key, parentId);
    }

    public Key apply(SortedKeyValueIterator<Key,Value> itr) throws IOException {
        Key key = itr.getTopKey();
        ByteSequence parentId = parsePointer(key);
        Text row = key.getRow();
        Key nextKey = key;
        do {
            key = nextKey;
            itr.next();
            nextKey = (itr.hasTop() ? itr.getTopKey() : null);
        } while (skip(nextKey, row, parentId));
        return getResult(key, parentId);
    }

    @Override
    protected ByteSequence parsePointer(Key current) {
        return parseRootPointerFromFI(current.getColumnQualifierData());
    }

    @Override
    protected Key getResult(Key current, ByteSequence pointer) {
        return TLD.buildParentKey(current.getRow(), pointer, TLD.parseFieldAndValueFromFI(current.getColumnFamilyData(), current.getColumnQualifierData()),
                        current.getColumnVisibility(), current.getTimestamp());
    }

    @Override
    protected boolean skip(Key next, Text row, ByteSequence pointer) {
        return next != null && isFi(next.getColumnFamilyData()) && pointer.equals(parseRootPointerFromFI(next.getColumnQualifierData()));
    }

    @Override
    protected Key getSeekStartKey(Key current, ByteSequence pointer) {
        int lastNullIndex = current.getColumnQualifier().toString().lastIndexOf(Constants.NULL);
        lastNullIndex = current.getColumnQualifier().toString().lastIndexOf(Constants.NULL, lastNullIndex - 1);
        String prefix = current.getColumnQualifier().toString().substring(0, lastNullIndex + 1);
        return new Key(current.getRow(), current.getColumnFamily(), new Text(prefix + pointer + Constants.MAX_UNICODE_STRING));
    }

    public boolean isFi(ByteSequence byteSeq) {
        byte[] bytes = byteSeq.getBackingArray();
        int offset = byteSeq.offset();
        return byteSeq.length() >= 3 && bytes[offset] == 'f' && bytes[offset + 1] == 'i' && bytes[offset + 2] == '\0';
    }
}
