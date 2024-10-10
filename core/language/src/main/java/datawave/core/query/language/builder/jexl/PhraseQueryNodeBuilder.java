package datawave.core.query.language.builder.jexl;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TokenizedPhraseQueryNode;
import org.apache.lucene.search.PhraseQuery;

import datawave.core.query.language.parser.jexl.JexlNode;

/**
 * Builds a {@link PhraseQuery} object from a {@link TokenizedPhraseQueryNode} object.
 */
public class PhraseQueryNodeBuilder implements QueryBuilder {

    public JexlNode build(QueryNode queryNode) throws QueryNodeException {
        throw new UnsupportedOperationException(getClass().getName() + " not implemented");
    }

}
