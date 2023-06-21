package datawave.query.jexl.functions;

import java.io.IOException;
import java.util.Set;

import datawave.query.attributes.Cardinality;
import datawave.query.attributes.FieldValueCardinality;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.tld.TLD;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import datawave.query.data.parsers.DatawaveKey;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;

public class CardinalityAggregator extends IdentityAggregator {

    private static final Logger log = Logger.getLogger(CardinalityAggregator.class);
    private static final Text EMPTY_TEXT = new Text();
    private boolean setDocIds;

    public CardinalityAggregator(Set<String> fieldsToKeep, boolean setDocIds) {
        super(fieldsToKeep);
        this.setDocIds = setDocIds;
    }

    @Override
    public Key apply(SortedKeyValueIterator<Key,Value> itr, Document doc, AttributeFactory attrs) throws IOException {
        Key key = itr.getTopKey();
        Text row = key.getRow();
        ByteSequence pointer = parsePointer(key.getColumnQualifierData());
        Key nextKey = key;
        while (nextKey != null && samePointer(row, pointer, nextKey)) {
            DatawaveKey topKey = new DatawaveKey(nextKey);
            String field = topKey.getFieldName();
            String value = topKey.getFieldValue();

            FieldValueCardinality fvC = null;
            byte[] currentValue = itr.getTopValue().get();
            try {
                if (currentValue.length > 0) {
                    fvC = new FieldValueCardinality(HyperLogLogPlus.Builder.build(currentValue));
                    if (log.isTraceEnabled()) {
                        log.trace("Set cardinality from FI value");
                    }
                }
            } catch (Exception e) {
                if (log.isTraceEnabled()) {
                    log.trace("Exception encountered " + e);
                }
            }

            if (null == fvC) {
                if (log.isTraceEnabled())
                    log.trace("Building cardinality for " + topKey.getUid());
                fvC = new FieldValueCardinality();
                if (setDocIds)
                    fvC.setDocId(topKey.getUid());
            }

            fvC.setContent(value);

            // for cardinalities, only use the visibility metadata
            Key metadata = new Key(EMPTY_TEXT, EMPTY_TEXT, EMPTY_TEXT, itr.getTopKey().getColumnVisibility(), -1);

            Cardinality card = new Cardinality(fvC, metadata, doc.isToKeep());

            // only keep fields that are index only
            card.setToKeep(fieldsToKeep == null || fieldsToKeep.contains(JexlASTHelper.removeGroupingContext(field)));
            doc.put(field, card);

            key = nextKey;
            itr.next();
            nextKey = (itr.hasTop() ? itr.getTopKey() : null);
        }
        return TLD.buildParentKey(row, pointer, TLD.parseFieldAndValueFromFI(key.getColumnFamilyData(), key.getColumnQualifierData()),
                        key.getColumnVisibility(), key.getTimestamp());
    }
}
