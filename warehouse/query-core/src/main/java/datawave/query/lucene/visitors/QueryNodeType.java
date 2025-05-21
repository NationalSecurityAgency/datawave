package datawave.query.lucene.visitors;

import java.util.HashMap;
import java.util.Map;

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

    /**
     * Returns the {@link QueryNodeType} for the given {@link QueryNode} class, or null if one does not exist.
     *
     * @param clazz
     *            the type
     * @return the {@link QueryNodeType}
     */
    public static QueryNodeType get(Class<? extends QueryNode> clazz) {
        return get(clazz.getName());
    }

    /**
     * Returns the {@link QueryNodeType} for the given class name, or null if one does not exist.
     *
     * @param className
     *            the class name
     * @return the {@link QueryNodeType}
     */
    public static QueryNodeType get(String className) {
        return ENUM_MAP.get(className);
    }
}
