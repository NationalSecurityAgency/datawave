package datawave.query.language.builder.jexl;

import java.util.List;

import datawave.query.language.functions.jexl.AtomValuesMatchFunction;
import datawave.query.language.functions.jexl.Compare;
import datawave.query.language.functions.jexl.DateFunction;
import datawave.query.language.functions.jexl.EvaluationOnly;
import datawave.query.language.functions.jexl.ExcerptFields;
import datawave.query.language.functions.jexl.Exclude;
import datawave.query.language.functions.jexl.GeoFunction;
import datawave.query.language.functions.jexl.Geowave.Contains;
import datawave.query.language.functions.jexl.Geowave.CoveredBy;
import datawave.query.language.functions.jexl.Geowave.Covers;
import datawave.query.language.functions.jexl.Geowave.Crosses;
import datawave.query.language.functions.jexl.Geowave.Intersects;
import datawave.query.language.functions.jexl.Geowave.Overlaps;
import datawave.query.language.functions.jexl.Geowave.Within;
import datawave.query.language.functions.jexl.GetAllMatches;
import datawave.query.language.functions.jexl.GroupBy;
import datawave.query.language.functions.jexl.Include;
import datawave.query.language.functions.jexl.IsNotNull;
import datawave.query.language.functions.jexl.IsNull;
import datawave.query.language.functions.jexl.Jexl;
import datawave.query.language.functions.jexl.JexlQueryFunction;
import datawave.query.language.functions.jexl.Loaded;
import datawave.query.language.functions.jexl.MatchesAtLeastCountOf;
import datawave.query.language.functions.jexl.MatchesInGroupFunction;
import datawave.query.language.functions.jexl.MatchesInGroupLeft;
import datawave.query.language.functions.jexl.NoExpansion;
import datawave.query.language.functions.jexl.OccurrenceFunction;
import datawave.query.language.functions.jexl.Options;
import datawave.query.language.functions.jexl.Text;
import datawave.query.language.functions.jexl.TimeFunction;
import datawave.query.language.functions.jexl.Unique;
import datawave.query.language.functions.jexl.UniqueByDay;
import datawave.query.language.functions.jexl.UniqueByHour;
import datawave.query.language.functions.jexl.UniqueByMinute;
import datawave.query.language.functions.jexl.UniqueByMonth;
import datawave.query.language.functions.jexl.UniqueByTenthOfHour;
import datawave.query.language.functions.jexl.UniqueByYear;
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

import com.google.common.collect.ImmutableList;

public class JexlTreeBuilder extends QueryTreeBuilder {
    
    public static final JexlQueryFunction[] DEFAULT_ALLOWED_FUNCTIONS = {new IsNull(), new IsNotNull(), new Include(), new Exclude(), new Text(),
            new GeoFunction(), new Contains(), new CoveredBy(), new Covers(), new Crosses(), new Intersects(), new Overlaps(), new Within(), new Loaded(),
            new DateFunction(), new OccurrenceFunction(), new MatchesInGroupFunction(), new MatchesInGroupLeft(), new GetAllMatches(),
            new MatchesAtLeastCountOf(), new Jexl(), new TimeFunction(), new AtomValuesMatchFunction(), new Options(), new Unique(), new GroupBy(),
            new EvaluationOnly(), new UniqueByDay(), new UniqueByHour(), new UniqueByTenthOfHour(), new UniqueByMonth(), new UniqueByYear(),
            new UniqueByMinute(), new NoExpansion(), new Compare(), new ExcerptFields()};
    
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
