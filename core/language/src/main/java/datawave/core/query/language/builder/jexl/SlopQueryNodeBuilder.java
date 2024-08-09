package datawave.core.query.language.builder.jexl;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;
import org.apache.lucene.search.Query;

import datawave.core.query.language.parser.jexl.JexlNode;
import datawave.core.query.language.parser.jexl.JexlPhraseNode;
import datawave.core.query.language.parser.jexl.JexlSelectorNode;
import datawave.core.query.language.parser.jexl.JexlWithinNode;

/**
 * This builder basically reads the {@link Query} object set on the {@link SlopQueryNode} child using {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} and
 * applies the slop value defined in the {@link SlopQueryNode}.
 */
public class SlopQueryNodeBuilder implements QueryBuilder {

    public JexlNode build(QueryNode queryNode) throws QueryNodeException {
        JexlNode returnNode = null;

        SlopQueryNode phraseSlopNode = (SlopQueryNode) queryNode;

        JexlNode node = (JexlNode) phraseSlopNode.getChild().getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);

        if (node instanceof JexlPhraseNode) {
            JexlPhraseNode phraseNode = (JexlPhraseNode) node;
            returnNode = new JexlWithinNode(phraseNode.getField(), phraseNode.getWordList(), phraseSlopNode.getValue());
        } else if (node instanceof JexlSelectorNode) {
            // if phrase only contained one word, a JexlSelectorNode would be created
            // and then a SlopQueryNode / within makes no sense
            returnNode = node;
        } else {
            throw new UnsupportedOperationException(node.getClass().getName() + " found as a child of a SlopQueryNode -- not implemented");
        }

        return returnNode;
    }

}
