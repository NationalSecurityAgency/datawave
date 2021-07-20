package datawave.query.language.builder.lucene;

import java.util.List;

import datawave.query.language.functions.lucene.LuceneQueryFunction;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FunctionQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TokenizedPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;

@Deprecated
public class AccumuloQueryTreeBuilder extends QueryTreeBuilder {
    public AccumuloQueryTreeBuilder() {
        setBuilder(TermRangeQueryNode.class, new RangeQueryNodeBuilder());
        setBuilder(GroupQueryNode.class, new GroupQueryNodeBuilder());
        setBuilder(FunctionQueryNode.class, new FunctionQueryNodeBuilder());
        setBuilder(FieldQueryNode.class, new FieldQueryNodeBuilder());
        setBuilder(BooleanQueryNode.class, new BooleanQueryNodeBuilder());
        setBuilder(ModifierQueryNode.class, new ModifierQueryNodeBuilder());
        setBuilder(TokenizedPhraseQueryNode.class, new PhraseQueryNodeBuilder());
        setBuilder(SlopQueryNode.class, new SlopQueryNodeBuilder());
    }
    
    public AccumuloQueryTreeBuilder(List<LuceneQueryFunction> allowedFunctions) {
        setBuilder(TermRangeQueryNode.class, new RangeQueryNodeBuilder());
        setBuilder(GroupQueryNode.class, new GroupQueryNodeBuilder());
        setBuilder(FunctionQueryNode.class, new FunctionQueryNodeBuilder(allowedFunctions));
        setBuilder(FieldQueryNode.class, new FieldQueryNodeBuilder());
        setBuilder(BooleanQueryNode.class, new BooleanQueryNodeBuilder());
        setBuilder(ModifierQueryNode.class, new ModifierQueryNodeBuilder());
        setBuilder(TokenizedPhraseQueryNode.class, new PhraseQueryNodeBuilder());
        setBuilder(SlopQueryNode.class, new SlopQueryNodeBuilder());
    }
    
    @Override
    public datawave.query.language.tree.QueryNode build(QueryNode queryNode) throws QueryNodeException {
        return (datawave.query.language.tree.QueryNode) super.build(queryNode);
    }
}
