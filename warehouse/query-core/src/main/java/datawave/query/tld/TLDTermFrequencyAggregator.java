package datawave.query.tld;

import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;

import datawave.query.jexl.functions.TermFrequencyAggregator;
import datawave.query.predicate.EventDataQueryFilter;

/**
 * TermFrequencyAggregator which will treat all TF uid's as the TLD uid for the purposes of aggregation
 */
public class TLDTermFrequencyAggregator extends TermFrequencyAggregator {

    public TLDTermFrequencyAggregator(Set<String> fieldsToKeep, EventDataQueryFilter attrFilter, int maxNextCount) {
        super(fieldsToKeep, attrFilter, maxNextCount);
    }

    /**
     * Parses out the datatype and root uid from a term frequency column qualifier
     * <p>
     * <code>datatype\0parent.document.id\0value\0FIELD</code>
     * <p>
     * <code>datatype\0parent.document.id.child.grandchild\0value\0FIELD</code>
     *
     * @param cq
     *            a term frequency column qualifier
     * @return the datatype and root uid from the column qualifier
     */
    @Override
    protected ByteSequence parsePointer(ByteSequence cq) {
        int nullCount = 0;
        int dotCount = 0;
        int index = 0;
        for (int i = 0; i < cq.length(); i++) {
            if ((cq.byteAt(i) == '\u0000' && ++nullCount == 2) || (cq.byteAt(i) == '.' && ++dotCount == 3)) {
                index = i;
                break;
            }
        }
        return cq.subSequence(0, index);
    }
}
