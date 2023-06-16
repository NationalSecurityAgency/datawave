package datawave.query.tld;

import datawave.query.jexl.functions.TermFrequencyAggregator;
import datawave.query.predicate.EventDataQueryFilter;
import org.apache.accumulo.core.data.ByteSequence;

import java.util.ArrayList;
import java.util.Set;

/**
 * TermFrequencyAggregator which will treat all TF uid's as the TLD uid for the purposes of aggregation
 */
public class TLDTermFrequencyAggregator extends TermFrequencyAggregator {

    public TLDTermFrequencyAggregator(Set<String> fieldsToKeep, EventDataQueryFilter attrFilter, int maxNextCount) {
        super(fieldsToKeep, attrFilter, maxNextCount);
    }

    @Override
    protected ByteSequence parsePointer(ByteSequence qualifier) {
        ArrayList<Integer> deezNulls = TLD.instancesOf(0, qualifier, -1);
        final int stop = deezNulls.get(1);
        final int uidStart = deezNulls.get(0);
        ByteSequence uid = qualifier.subSequence(uidStart + 1, stop);
        ArrayList<Integer> deezDots = TLD.instancesOf('.', uid);
        if (deezDots.size() > 2) {
            return qualifier.subSequence(0, uidStart + deezDots.get(2) + 1);
        } else {
            return qualifier.subSequence(0, stop);
        }
    }
}
