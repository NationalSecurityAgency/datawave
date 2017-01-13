package nsa.datawave.query.language.builder.jexl;

import java.util.List;

import nsa.datawave.query.language.functions.jexl.AtomValuesMatchFunction;
import nsa.datawave.query.language.functions.jexl.DateFunction;
import nsa.datawave.query.language.functions.jexl.Exclude;
import nsa.datawave.query.language.functions.jexl.GeoFunction;
import nsa.datawave.query.language.functions.jexl.GetAllMatches;
import nsa.datawave.query.language.functions.jexl.Include;
import nsa.datawave.query.language.functions.jexl.IsNotNull;
import nsa.datawave.query.language.functions.jexl.IsNull;
import nsa.datawave.query.language.functions.jexl.Jexl;
import nsa.datawave.query.language.functions.jexl.JexlQueryFunction;
import nsa.datawave.query.language.functions.jexl.Loaded;
import nsa.datawave.query.language.functions.jexl.MatchesAtLeastCountOf;
import nsa.datawave.query.language.functions.jexl.MatchesInGroupFunction;
import nsa.datawave.query.language.functions.jexl.MatchesInGroupLeft;
import nsa.datawave.query.language.functions.jexl.OccurrenceFunction;
import nsa.datawave.query.language.functions.jexl.TimeFunction;
import nsa.datawave.query.language.parser.jexl.JexlNode;

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

import com.google.common.collect.ImmutableList;

public class JexlTreeBuilder extends QueryTreeBuilder {
    
    public static final JexlQueryFunction[] DEFAULT_ALLOWED_FUNCTIONS = {new IsNull(), new IsNotNull(), new Include(), new Exclude(), new GeoFunction(),
            new Loaded(), new DateFunction(), new OccurrenceFunction(), new MatchesInGroupFunction(), new MatchesInGroupLeft(), new GetAllMatches(),
            new MatchesAtLeastCountOf(), new Jexl(), new TimeFunction(), new AtomValuesMatchFunction()};
    
    public static final List<JexlQueryFunction> DEFAULT_ALLOWED_FUNCTION_LIST;
    
    static {
        DEFAULT_ALLOWED_FUNCTION_LIST = ImmutableList.<JexlQueryFunction> builder().add(DEFAULT_ALLOWED_FUNCTIONS).build();
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
