package datawave.core.query.language.builder.lucene;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.RangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;
import org.apache.lucene.search.TermRangeQuery;

import datawave.core.query.language.tree.SelectorNode;
import datawave.core.query.search.RangeFieldedTerm;

/**
 * Builds a {@link TermRangeQuery} object from a {@link RangeQueryNode} object.
 */
@Deprecated
public class RangeQueryNodeBuilder implements QueryBuilder {

    public datawave.core.query.language.tree.QueryNode build(QueryNode queryNode) throws QueryNodeException {
        TermRangeQueryNode rangeNode = (TermRangeQueryNode) queryNode;
        FieldQueryNode upper = rangeNode.getUpperBound();
        FieldQueryNode lower = rangeNode.getLowerBound();

        String field = rangeNode.getField().toString();

        RangeFieldedTerm rangeQuery = new RangeFieldedTerm(field, lower.getTextAsString(), upper.getTextAsString(), rangeNode.isLowerInclusive(),
                        rangeNode.isUpperInclusive());

        return new SelectorNode(rangeQuery);
    }

}
