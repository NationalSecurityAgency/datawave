package datawave.query.function;

import datawave.query.attributes.Cardinality;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.attributes.Document;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;

/**
 *
 */
public class MergeSummarization extends CardinalitySummation {

    public Logger log = Logger.getLogger(MergeSummarization.class);

    public MergeSummarization(Key topKey, Document refDoc) {
        super(topKey, refDoc);
    }

    @Override
    protected void merge(Cardinality originalCardinality, Cardinality cardinalityToMerge, DatawaveKey keyParser, boolean merge) {
        try {

            if (log.isTraceEnabled())
                log.trace(originalCardinality.getData() + " Before merge " + originalCardinality.getContent().getEstimate().cardinality() + " " + keyParser);
            originalCardinality.getContent().merge(cardinalityToMerge.getContent());
            if (log.isTraceEnabled())
                log.trace(keyParser.getFieldName() + " After merge " + originalCardinality.getContent().getEstimate().cardinality() + " " + keyParser);
        } catch (CardinalityMergeException e) {
            throw new RuntimeException(e);
        }
    }
}
