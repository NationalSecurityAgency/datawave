package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.AnyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.BoostQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.DeletedQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FunctionQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchAllDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchNoDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.NoTokenFoundQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.NotBooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OpaqueQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OrQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.PathQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.PhraseSlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ProximityQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TokenizedPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.AbstractRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.BooleanModifierNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.MultiPhraseQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.PrefixWildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.SynonymQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;

import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates the various known concrete implementations of {@link QueryNode}.
 */
public enum QueryNodeType {
    // @formatter:off
    AND(AndQueryNode.class),
    ANY(AnyQueryNode.class),
    FIELD(FieldQueryNode.class),
    BOOLEAN(BooleanQueryNode.class),
    BOOST(BoostQueryNode.class),
    DELETED(DeletedQueryNode.class),
    FUZZY(FuzzyQueryNode.class),
    GROUP(GroupQueryNode.class),
    MATCH_ALL_DOCS(MatchAllDocsQueryNode.class),
    MATCH_NO_DOCS(MatchNoDocsQueryNode.class),
    MODIFIER(ModifierQueryNode.class),
    NO_TOKEN_FOUND(NoTokenFoundQueryNode.class),
    OPAQUE(OpaqueQueryNode.class),
    OR(OrQueryNode.class),
    PATH(PathQueryNode.class),
    PHRASE_SLOP(PhraseSlopQueryNode.class),
    PROXIMITY(ProximityQueryNode.class),
    QUOTED_FIELD(QuotedFieldQueryNode.class),
    SLOP(SlopQueryNode.class),
    TOKENIZED_PHRASE(TokenizedPhraseQueryNode.class),
    ABSTRACT_RANGE(AbstractRangeQueryNode.class), // Included because AbstractRangeQueryNode is not actually an abstract class.
    BOOLEAN_MODIFIER(BooleanModifierNode.class),
    MULTI_PHRASE(MultiPhraseQueryNode.class),
    POINT(PointQueryNode.class),
    POINT_RANGE(PointRangeQueryNode.class),
    PREFIX_WILDCARD(PrefixWildcardQueryNode.class),
    REGEX(RegexpQueryNode.class),
    SYNONYM(SynonymQueryNode.class),
    TERM_RANGE(TermRangeQueryNode.class),
    WILDCARD(WildcardQueryNode.class),
    FUNCTION(FunctionQueryNode.class),
    NOT_BOOLEAN(NotBooleanQueryNode.class);
    // @formatter:on
    
    private final String className;
    
    QueryNodeType(Class<? extends QueryNode> clazz) {
        this.className = clazz.getName();
    }
    
    private static final Map<String,QueryNodeType> ENUM_MAP;
    static {
        Map<String,QueryNodeType> map = new HashMap<>();
        for (QueryNodeType type : QueryNodeType.values()) {
            map.put(type.className, type);
        }
        ENUM_MAP = map;
    }

    public static BaseVisitor getVisitorFromQueryNode(QueryNode node) {
        QueryNodeType type = get(node.getClass());
        switch (type) {
            case AND:
                return new ValidateAndVisitor();
            case ANY:
                return new ValidateAnyVisitor();
            case FIELD:
                return new ValidateFieldVisitor();
            case BOOLEAN:
                return new ValidateBooleanVisitor();
            case BOOST:
                return new ValidateBoostVisitor();
            case DELETED:
                return new ValidateDeletedVisitor();
            case FUZZY:
                return new ValidateFuzzyVisitor();
            case GROUP:
                return new ValidateGroupVisitor();
            case MATCH_ALL_DOCS:
                return new ValidateMatchAllDocsVisitor();
            case MATCH_NO_DOCS:
                return new ValidateMatchNoDocsVisitor();
            case MODIFIER:
                return new ValidateModifierVisitor();
            case NO_TOKEN_FOUND:
                return new ValidateNoTokenFoundVisitor();
            case OPAQUE:
                return new ValidateOpaqueVisitor();
            case OR:
                return new ValidateOrVisitor();
            case PATH:
                return new ValidatePathVisitor();
            case PHRASE_SLOP:
                return new ValidatePhraseSlopVisitor();
            case PROXIMITY:
                return new ValidateProximityVisitor();
            case QUOTED_FIELD:
                return new ValidateQuotedFieldVisitor();
            case SLOP:
                return new ValidateSlopVisitor();
            case TOKENIZED_PHRASE:
                return new ValidateTokenizedPhraseVisitor();
            case ABSTRACT_RANGE:
                return new ValidateAbstractRangeVisitor();
            case BOOLEAN_MODIFIER:
                return new ValidateBooleanModifierVisitor();
            case MULTI_PHRASE:
                return new ValidateMultiPhraseVisitor();
            case POINT:
                return new ValidatePointVisitor();
            case POINT_RANGE:
                return new ValidatePointRangeVisitor();
            case PREFIX_WILDCARD:
                return new ValidatePrefixWildcardVisitor();
            case REGEX:
                return new ValidateRegexVisitor();
            case SYNONYM:
                return new ValidateSynonymVisitor();
            case TERM_RANGE:
                return new ValidateTermRangeVisitor();
            case WILDCARD:
                return new ValidateWildcardVisitor();
            case FUNCTION:
                return new ValidateFunctionVisitor();
            case NOT_BOOLEAN:
                return new ValidateNotVisitor();
            default:
                throw new IllegalArgumentException("No visitor for " + type);
        }
    }
    
    /**
     * Returns the {@link QueryNodeType} for the given {@link QueryNode} class, or null if one does not exist.
     * @param clazz the type
     * @return the {@link QueryNodeType}
     */
    public static QueryNodeType get(Class<? extends QueryNode> clazz) {
        return get(clazz.getName());
    }
    
    /**
     * Returns the {@link QueryNodeType} for the given class name, or null if one does not exist.
     * @param className the class name
     * @return the {@link QueryNodeType}
     */
    public static QueryNodeType get(String className) {
        return ENUM_MAP.get(className);
    }
}
