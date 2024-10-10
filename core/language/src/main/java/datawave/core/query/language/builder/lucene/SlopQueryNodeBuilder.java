package datawave.core.query.language.builder.lucene;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;
import org.apache.lucene.search.Query;

import datawave.core.query.language.tree.AdjNode;
import datawave.core.query.language.tree.SelectorNode;
import datawave.core.query.language.tree.WithinNode;

/**
 * This builder basically reads the {@link Query} object set on the {@link SlopQueryNode} child using {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} and
 * applies the slop value defined in the {@link SlopQueryNode}.
 */
@Deprecated
public class SlopQueryNodeBuilder implements QueryBuilder {

    public datawave.core.query.language.tree.QueryNode build(QueryNode queryNode) throws QueryNodeException {
        datawave.core.query.language.tree.QueryNode returnNode = null;

        SlopQueryNode phraseSlopNode = (SlopQueryNode) queryNode;

        datawave.core.query.language.tree.QueryNode node = (datawave.core.query.language.tree.QueryNode) phraseSlopNode.getChild()
                        .getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);

        if (node instanceof AdjNode) {
            datawave.core.query.language.tree.QueryNode[] nodeArray = new datawave.core.query.language.tree.QueryNode[node.getChildren().size()];
            returnNode = new WithinNode(phraseSlopNode.getValue(), node.getChildren().toArray(nodeArray));
        } else if (node instanceof SelectorNode) {
            // if phrase only contained one word, a SelectorNode would be created
            // and then a SlopQueryNode / within makes no sense
            returnNode = node;
        } else {
            throw new UnsupportedOperationException(node.getClass().getName() + " found as a child of a SlopQueryNode -- not implemented");
        }

        return returnNode;
    }

}
