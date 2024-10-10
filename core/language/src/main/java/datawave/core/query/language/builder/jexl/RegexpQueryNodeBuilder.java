package datawave.core.query.language.builder.jexl;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.search.TermQuery;

import datawave.core.query.language.parser.jexl.JexlNode;
import datawave.core.query.language.parser.jexl.JexlSelectorNode;

/**
 * Builds a {@link TermQuery} object from a {@link FieldQueryNode} object.
 */
public class RegexpQueryNodeBuilder implements QueryBuilder {

    public JexlNode build(QueryNode queryNode) throws QueryNodeException {
        JexlNode returnNode = null;

        if (queryNode instanceof RegexpQueryNode) {
            RegexpQueryNode regexpQueryNode = (RegexpQueryNode) queryNode;
            String field = regexpQueryNode.getFieldAsString();
            UnescapedCharSequence ecs = (UnescapedCharSequence) regexpQueryNode.getText();

            if (field == null || field.isEmpty()) {
                returnNode = new JexlSelectorNode(JexlSelectorNode.Type.REGEX, "", ecs.toString());
            } else {
                returnNode = new JexlSelectorNode(JexlSelectorNode.Type.REGEX, field, ecs.toString());
            }
        }

        return returnNode;
    }

}
