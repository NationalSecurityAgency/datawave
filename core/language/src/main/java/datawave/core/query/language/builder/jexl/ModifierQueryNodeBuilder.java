package datawave.core.query.language.builder.jexl;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.search.Query;

import datawave.core.query.language.parser.jexl.JexlNode;

/**
 * Builds no object, it only returns the {@link Query} object set on the {@link ModifierQueryNode} object using a
 * {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} tag.
 */
public class ModifierQueryNodeBuilder implements QueryBuilder {

    public JexlNode build(QueryNode queryNode) throws QueryNodeException {
        ModifierQueryNode modifierQueryNode = (ModifierQueryNode) queryNode;

        return (JexlNode) modifierQueryNode.getChild().getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);

    }
}
