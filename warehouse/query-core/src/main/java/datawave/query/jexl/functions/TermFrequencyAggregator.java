package datawave.query.jexl.functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import com.google.common.collect.Maps;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.tld.TLD;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang.StringUtils;
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
    protected Tuple2<String,String> parserFieldNameValue(Key topKey) {
        DatawaveKey parser = new DatawaveKey(topKey);
        return new Tuple2<String,String>(parser.getFieldName(), parser.getFieldValue());
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
    
    /**
     * Limit keep fields to those that are index only or if there is no filter specified
     * 
     * @param topKey
     * @param fieldNameValue
     * @return
     */
    @Override
    protected boolean toKeep(Key topKey, Tuple2<String,String> fieldNameValue) {
        return fieldsToKeep == null || fieldsToKeep.contains(JexlASTHelper.removeGroupingContext(fieldNameValue.first()));
    }
    
    /**
     * Only aggregate tf fields that are part of the returned document or necessary for query evaluation, not all TF
     * 
     * @param topKey
     * @param fieldNameValue
     * @param toKeep
     * @return
     */
    @Override
    protected boolean addToDoc(Key topKey, Tuple2<String,String> fieldNameValue, boolean toKeep) {
        return filter == null || filter.apply(Maps.immutableEntry(topKey, StringUtils.EMPTY));
    }
}
