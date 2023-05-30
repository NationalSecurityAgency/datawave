package datawave.query.language.builder.jexl;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import datawave.query.language.functions.jexl.JexlQueryFunction;
import datawave.query.language.parser.jexl.JexlNode;

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
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;

public class JexlTreeBuilder extends QueryTreeBuilder {
    
    public static final JexlQueryFunction[] DEFAULT_ALLOWED_FUNCTIONS;
    public static final List<JexlQueryFunction> DEFAULT_ALLOWED_FUNCTION_LIST;
    
    static {
        ClassPathScanningCandidateComponentProvider p = new ClassPathScanningCandidateComponentProvider(false);
        p.addIncludeFilter(new AssignableTypeFilter(JexlQueryFunction.class));
        Set<BeanDefinition> candidates = p.findCandidateComponents("datawave.query.language.functions.jexl");
        DEFAULT_ALLOWED_FUNCTION_LIST = candidates.stream().map(b -> {
            try {
                return (JexlQueryFunction) (Class.forName(b.getBeanClassName()).newInstance());
            } catch (Exception e) {
                return null;
            }
        }).filter(b -> b != null).collect(Collectors.toList());
        DEFAULT_ALLOWED_FUNCTIONS = DEFAULT_ALLOWED_FUNCTION_LIST.toArray(new JexlQueryFunction[0]);
    }
    
    public JexlTreeBuilder() {
        this(DEFAULT_ALLOWED_FUNCTION_LIST);
    }
    
    public JexlTreeBuilder(List<JexlQueryFunction> allowedFunctions) {
        setBuilder(GroupQueryNode.class, new GroupQueryNodeBuilder());
        setBuilder(FunctionQueryNode.class, new FunctionQueryNodeBuilder(allowedFunctions));
        setBuilder(FieldQueryNode.class, new FieldQueryNodeBuilder());
        setBuilder(BooleanQueryNode.class, new BooleanQueryNodeBuilder());
        setBuilder(ModifierQueryNode.class, new ModifierQueryNodeBuilder());
        setBuilder(TokenizedPhraseQueryNode.class, new PhraseQueryNodeBuilder());
        setBuilder(TermRangeQueryNode.class, new RangeQueryNodeBuilder());
        setBuilder(RegexpQueryNode.class, new RegexpQueryNodeBuilder());
        setBuilder(SlopQueryNode.class, new SlopQueryNodeBuilder());
    }
    
    @Override
    public JexlNode build(QueryNode queryNode) throws QueryNodeException {
        return (JexlNode) super.build(queryNode);
    }
}
