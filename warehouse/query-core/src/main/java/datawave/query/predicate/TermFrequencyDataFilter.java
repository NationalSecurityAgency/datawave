package datawave.query.predicate;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.JexlNode;

import datawave.query.util.TypeMetadata;

/**
 * A data filter that operates on TermFrequency keys
 */
public class TermFrequencyDataFilter extends EventDataQueryExpressionFilter {

    /**
     * Constructor matching {@link EventDataQueryExpressionFilter}
     *
     * @param node
     *            a node in the query tree
     * @param typeMetadata
     *            an instance of {@link TypeMetadata}
     * @param nonEventFields
     *            a set of non-event fields
     */
    public TermFrequencyDataFilter(JexlNode node, TypeMetadata typeMetadata, Set<String> nonEventFields) {
        super(node, typeMetadata, nonEventFields);
    }

    /**
     * TermFrequency ranges necessitate a full value match
     *
     * @param key
     *            a term frequency key
     * @return the same value as {@link #peek(Key)}
     */
    @Override
    public boolean keep(Key key) {
        // for things that will otherwise be added need to ensure it's actually a value match. This is necessary when dealing with TF ranges.
        return peek(key);
    }
}
