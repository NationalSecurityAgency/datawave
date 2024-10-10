package datawave.core.query.language.builder.jexl;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.RangeQueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;
import org.apache.lucene.search.TermRangeQuery;

import datawave.core.query.language.parser.jexl.JexlNode;
import datawave.core.query.language.parser.jexl.JexlRangeNode;

/**
 * Builds a {@link TermRangeQuery} object from a {@link RangeQueryNode} object.
 */
public class RangeQueryNodeBuilder implements QueryBuilder {

    public JexlNode build(QueryNode queryNode) throws QueryNodeException {
        TermRangeQueryNode rangeNode = (TermRangeQueryNode) queryNode;
        FieldQueryNode lower = rangeNode.getLowerBound();
        FieldQueryNode upper = rangeNode.getUpperBound();

        boolean lowerWildcard = lower.getTextAsString().equals("*");
        boolean upperWildcard = upper.getTextAsString().equals("*");
        if (lowerWildcard && upperWildcard) {
            throw new QueryNodeException(new MessageImpl("Wildcard of lower and upper bouds not allowed"));
        }

        String field = rangeNode.getField().toString();

        return new JexlRangeNode(field, lower.getTextAsString(), upper.getTextAsString(), rangeNode.isLowerInclusive(), rangeNode.isUpperInclusive());
    }

}
