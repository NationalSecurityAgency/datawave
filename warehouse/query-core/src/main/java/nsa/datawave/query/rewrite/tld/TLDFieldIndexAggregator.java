package nsa.datawave.query.rewrite.tld;

import static nsa.datawave.query.rewrite.tld.TLD.parsePointerFromFI;
import static nsa.datawave.query.rewrite.tld.TLD.parseRootPointerFromFI;

import java.io.IOException;
import java.util.Set;

import nsa.datawave.marking.ColumnVisibilityCache;
import nsa.datawave.query.rewrite.attributes.Attribute;
import nsa.datawave.query.rewrite.attributes.AttributeFactory;
import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.attributes.DocumentKey;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import nsa.datawave.query.rewrite.jexl.functions.FieldIndexAggregator;

import nsa.datawave.query.rewrite.predicate.EventDataQueryFilter;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

public class TLDFieldIndexAggregator implements FieldIndexAggregator {
    private Set<String> fieldsToAggregate;
    private EventDataQueryFilter attrFilter;
    
    public TLDFieldIndexAggregator(Set<String> fieldsToAggregate, EventDataQueryFilter attrFilter) {
        this.fieldsToAggregate = fieldsToAggregate;
        this.attrFilter = attrFilter;
    }
    
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Document d, AttributeFactory af) throws IOException {
        Key key = itr.getTopKey();
        ByteSequence parentId = parseRootPointerFromFI(key.getColumnQualifierData());
        ByteSequence docId = null;
        Key nextKey = key;
        do {
            key = nextKey;
            String field = key.getColumnFamily().toString().substring(3);
            String value = key.getColumnQualifier().toString();
            value = value.substring(0, value.indexOf('\0'));
            Attribute<?> attr = af.create(field, value, key, true);
            // only keep fields that are index only and pass the attribute filter
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
        } while (nextKey != null && isFi(nextKey.getColumnFamilyData()) && parentId.equals(parseRootPointerFromFI(nextKey.getColumnQualifierData())));
        return TLD.buildParentKey(key.getRow(), parentId, TLD.parseFieldAndValueFromFI(key.getColumnFamilyData(), key.getColumnQualifierData()),
                        key.getColumnVisibility(), key.getTimestamp());
    }
    
    public Key apply(SortedKeyValueIterator<Key,Value> itr) throws IOException {
        Key key = itr.getTopKey();
        ByteSequence parentId = parseRootPointerFromFI(key.getColumnQualifierData());
        Key nextKey = key;
        do {
            key = nextKey;
            itr.next();
            nextKey = (itr.hasTop() ? itr.getTopKey() : null);
        } while (nextKey != null && isFi(nextKey.getColumnFamilyData()) && parentId.equals(parseRootPointerFromFI(nextKey.getColumnQualifierData())));
        return TLD.buildParentKey(key.getRow(), parentId, TLD.parseFieldAndValueFromFI(key.getColumnFamilyData(), key.getColumnQualifierData()),
                        key.getColumnVisibility(), key.getTimestamp());
    }
    
    public boolean isFi(ByteSequence byteSeq) {
        byte[] bytes = byteSeq.getBackingArray();
        int offset = byteSeq.offset();
        return byteSeq.length() >= 3 && bytes[offset] == 'f' && bytes[offset + 1] == 'i' && bytes[offset + 2] == '\0';
    }
}
