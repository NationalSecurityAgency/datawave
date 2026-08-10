package datawave.query.lucene.visitors;

import java.util.List;

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

public class BaseVisitor {

    public static QueryNode copy(QueryNode node) {
        try {
            return node.cloneTree();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public Object visit(QueryNode node, Object data) {
        // The class QueryNode does not have an accept method to support the visitor pattern. The switch below is a workaround to ensure that we call the
        // correct visit() method based on the node's type.
        QueryNodeType type = QueryNodeType.get(node.getClass().getName());
        if (type != null) {
            switch (type) {
                case AND:
                    return visit((AndQueryNode) node, data);
                case ANY:
                    return visit((AnyQueryNode) node, data);
                case FIELD:
                    return visit((FieldQueryNode) node, data);
                case BOOLEAN:
                    return visit((BooleanQueryNode) node, data);
                case BOOST:
                    return visit((BoostQueryNode) node, data);
                case FUZZY:
                    return visit((FuzzyQueryNode) node, data);
                case GROUP:
                    return visit((GroupQueryNode) node, data);
                case MATCH_ALL_DOCS:
                    return visit((MatchAllDocsQueryNode) node, data);
                case MATCH_NO_DOCS:
                    return visit((MatchNoDocsQueryNode) node, data);
                case MODIFIER:
                    return visit((ModifierQueryNode) node, data);
                case NO_TOKEN_FOUND:
                    return visit((NoTokenFoundQueryNode) node, data);
                case OPAQUE:
                    return visit((OpaqueQueryNode) node, data);
                case OR:
                    return visit((OrQueryNode) node, data);
                case PATH:
                    return visit((PathQueryNode) node, data);
                case PHRASE_SLOP:
                    return visit((PhraseSlopQueryNode) node, data);
                case PROXIMITY:
                    return visit((ProximityQueryNode) node, data);
                case QUOTED_FIELD:
                    return visit((QuotedFieldQueryNode) node, data);
                case SLOP:
                    return visit((SlopQueryNode) node, data);
                case TOKENIZED_PHRASE:
                    return visit((TokenizedPhraseQueryNode) node, data);
                case ABSTRACT_RANGE:
                    // noinspection rawtypes
                    return visit((AbstractRangeQueryNode) node, data);
                case BOOLEAN_MODIFIER:
                    return visit((BooleanModifierNode) node, data);
                case MULTI_PHRASE:
                    return visit((MultiPhraseQueryNode) node, data);
                case POINT:
                    return visit((PointQueryNode) node, data);
                case POINT_RANGE:
                    return visit((PointRangeQueryNode) node, data);
                case PREFIX_WILDCARD:
                    return visit((PrefixWildcardQueryNode) node, data);
                case REGEX:
                    return visit((RegexpQueryNode) node, data);
                case SYNONYM:
                    return visit((SynonymQueryNode) node, data);
                case TERM_RANGE:
                    return visit((TermRangeQueryNode) node, data);
                case WILDCARD:
                    return visit((WildcardQueryNode) node, data);
                case FUNCTION:
                    return visit((FunctionQueryNode) node, data);
                case NOT_BOOLEAN:
                    return visit((NotBooleanQueryNode) node, data);
                case DELETED:
                    return visit((DeletedQueryNode) node, data);
                default:
                    throw new UnsupportedOperationException("No visit() method defined for " + QueryNodeType.class.getSimpleName() + " " + type);
            }
        } else {
            throw new UnsupportedOperationException("No " + QueryNodeType.class.getSimpleName() + " constant defined for class " + node.getClass());
        }
    }

    public Object visit(AndQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(AnyQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(FieldQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(BooleanQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(BoostQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(DeletedQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(FuzzyQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(GroupQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(MatchAllDocsQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(MatchNoDocsQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(ModifierQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(NoTokenFoundQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(OpaqueQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(OrQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(PathQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(PhraseSlopQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(ProximityQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(QuotedFieldQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(SlopQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(TokenizedPhraseQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    @SuppressWarnings("rawtypes")
    public Object visit(AbstractRangeQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(BooleanModifierNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(MultiPhraseQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(PointQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(PointRangeQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(PrefixWildcardQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(RegexpQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(SynonymQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(TermRangeQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(WildcardQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(FunctionQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    public Object visit(NotBooleanQueryNode node, Object data) {
        visitChildren(node, data);
        return data;
    }

    protected void visitChildren(QueryNode node, Object data) {
        List<QueryNode> children = node.getChildren();
        if (children != null) {
            for (QueryNode child : children) {
                visit(child, data);
            }
        }
    }

}
