package datawave.query.lucene.visitors;

import java.util.List;
import java.util.Locale;

import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.AnyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.BoostQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.DeletedQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldValuePairQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FunctionQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.LuceneQueryNodeHelper;
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
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
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

import datawave.query.language.parser.lucene.EscapeQuerySyntaxImpl;

/**
 * A visitor implementation that returns a formatted LUCENE query string from a given QueryNode. This visitor acts as an equivalent to
 * {@link QueryNode#toQueryString(EscapeQuerySyntax)} with some differences:
 * <ul>
 * <li>Unfielded terms such as in {@code 'FOO:abc def'} will be formatted to {@code 'FOO:abc def'} instead of {@code 'FOO:abc :def'}.</li>
 * <li>The NOT operator such as {@code 'FOO:abc NOT BAR:def'} will be formatted to {@code 'FOO:abc NOT BAR:def'} instead of {@code 'FOO:abc -BAR:def'}.</li>
 * </ul>
 */
public class LuceneQueryStringBuildingVisitor extends BaseVisitor {

    private static final EscapeQuerySyntax escapedSyntax = new EscapeQuerySyntaxImpl();

    public static String build(QueryNode node) {
        LuceneQueryStringBuildingVisitor visitor = new LuceneQueryStringBuildingVisitor();
        return ((StringBuilder) visitor.visit(node, new StringBuilder())).toString();
    }

    @Override
    public Object visit(AndQueryNode node, Object data) {
        return visitJunctionNode(node, data, " AND ");
    }

    @Override
    public Object visit(AnyQueryNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        String field = node.getFieldAsString();
        boolean isDefaultField = LuceneQueryNodeHelper.isDefaultField(node, node.getField());
        if (!isDefaultField) {
            sb.append(field).append(":(");
        }

        sb.append("( ");
        sb.append(joinChildren(node, " "));
        sb.append(" )");
        sb.append(" ANY ");
        sb.append(node.getMinimumMatchingElements());

        if (!isDefaultField) {
            sb.append(")");
        }
        return sb;
    }

    @Override
    public Object visit(FieldQueryNode node, Object data) {
        return visitField(node, data);
    }

    private Object visitField(FieldQueryNode node, Object data) {
        String field = node.getFieldAsString();
        StringBuilder sb = (StringBuilder) data;
        boolean isDefaultField = LuceneQueryNodeHelper.isDefaultField(node, node.getField());
        if (!isDefaultField) {
            sb.append(field).append(":");
        }
        sb.append(escape(node.getText(), Locale.getDefault(), EscapeQuerySyntax.Type.NORMAL));
        return sb;
    }

    @Override
    public Object visit(BooleanQueryNode node, Object data) {
        return visitJunctionNode(node, data, " ");
    }

    @Override
    public Object visit(BoostQueryNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        QueryNode child = node.getChild();
        if (child != null) {
            visit(child, sb);
            sb.append("^");
            sb.append(getFloatStr(node.getValue()));
        }
        return sb;
    }

    @Override
    public Object visit(DeletedQueryNode node, Object data) {
        ((StringBuilder) data).append("[DELETEDCHILD]");
        return data;
    }

    @Override
    public Object visit(FuzzyQueryNode node, Object data) {
        String field = node.getFieldAsString();
        StringBuilder sb = (StringBuilder) data;
        boolean isDefaultField = LuceneQueryNodeHelper.isDefaultField(node, node.getField());
        if (!isDefaultField) {
            sb.append(field).append(":");
        }
        sb.append(escape(node.getText(), Locale.getDefault(), EscapeQuerySyntax.Type.NORMAL));
        sb.append("~").append(node.getSimilarity());
        return sb;
    }

    @Override
    public Object visit(GroupQueryNode node, Object data) {
        QueryNode child = node.getChild();
        if (child != null) {
            StringBuilder sb = (StringBuilder) data;
            sb.append("( ");
            visit(child, sb);
            sb.append(" )");
        }
        return data;
    }

    @Override
    public Object visit(MatchAllDocsQueryNode node, Object data) {
        ((StringBuilder) data).append("*:*");
        return data;
    }

    @Override
    public Object visit(MatchNoDocsQueryNode node, Object data) {
        // MatchNoDocsQueryNode does not override toQueryString(). Default to behavior of its parent class DeletedQueryNode.
        return visit((DeletedQueryNode) node, data);
    }

    @Override
    public Object visit(ModifierQueryNode node, Object data) {
        QueryNode child = node.getChild();
        if (child != null) {
            StringBuilder sb = (StringBuilder) data;
            ModifierQueryNode.Modifier modifier = node.getModifier();
            if (child instanceof ModifierQueryNode) {
                sb.append("(");
                sb.append(modifier.toLargeString());
                visit(child, sb);
                sb.append(")");
            } else {
                sb.append(modifier.toLargeString());
                visit(child, sb);
            }
        }
        return data;
    }

    @Override
    public Object visit(NoTokenFoundQueryNode node, Object data) {
        ((StringBuilder) data).append("[NTF]");
        return data;
    }

    @Override
    public Object visit(OpaqueQueryNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("@");
        sb.append(node.getSchema());
        sb.append(":'");
        sb.append(node.getValue());
        sb.append("'");
        return sb;
    }

    @Override
    public Object visit(OrQueryNode node, Object data) {
        return visitJunctionNode(node, data, " OR ");
    }

    @Override
    public Object visit(PathQueryNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("/").append(node.getFirstPathElement());
        for (PathQueryNode.QueryText element : node.getPathElements(1)) {
            sb.append("/\"").append(escape(element.getValue(), Locale.getDefault(), EscapeQuerySyntax.Type.STRING)).append("\"");
        }
        return sb;
    }

    @Override
    public Object visit(PhraseSlopQueryNode node, Object data) {
        QueryNode child = node.getChild();
        if (child != null) {
            StringBuilder sb = (StringBuilder) data;
            visit(child, sb);
            sb.append("~");
            sb.append(getFloatStr((float) node.getValue()));
        }
        return data;
    }

    @Override
    public Object visit(ProximityQueryNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        String field = node.getFieldAsString();
        boolean isDefaultField = LuceneQueryNodeHelper.isDefaultField(node, node.getField());
        if (!isDefaultField) {
            sb.append(field).append(":(");
        }
        sb.append("( ");
        sb.append(joinChildren(node, " "));
        sb.append(" ) ");
        ProximityQueryNode.Type proximityType = node.getProximityType();
        switch (proximityType) {
            case PARAGRAPH:
                sb.append("WITHIN PARAGRAPH");
                break;
            case SENTENCE:
                sb.append("WITHIN SENTENCE");
                break;
            case NUMBER:
                sb.append("WITHIN");
                break;
            default:
        }
        if (node.getDistance() > -1) {
            sb.append(" ").append(node.getDistance());
        }
        if (node.isInOrder()) {
            sb.append(" INORDER");
        }
        if (!isDefaultField) {
            sb.append(")");
        }
        return sb;
    }

    @Override
    public Object visit(QuotedFieldQueryNode node, Object data) {
        String field = node.getFieldAsString();
        StringBuilder sb = (StringBuilder) data;
        boolean isDefaultField = LuceneQueryNodeHelper.isDefaultField(node, node.getField());
        if (!isDefaultField) {
            sb.append(field).append(":");
        }
        sb.append("\"").append(escape(node.getText(), Locale.getDefault(), EscapeQuerySyntax.Type.STRING)).append("\"");
        return sb;
    }

    @Override
    public Object visit(SlopQueryNode node, Object data) {
        QueryNode child = node.getChild();
        if (child != null) {
            StringBuilder sb = (StringBuilder) data;
            visit(child, sb);
            sb.append("~");
            sb.append(getFloatStr((float) node.getValue()));
        }
        return data;
    }

    @Override
    public Object visit(TokenizedPhraseQueryNode node, Object data) {
        List<QueryNode> children = node.getChildren();
        if (children != null && !children.isEmpty()) {
            StringBuilder sb = (StringBuilder) data;
            sb.append("[TP[");
            sb.append(joinChildren(node, ","));
            sb.append("]]");
        }
        return data;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Object visit(AbstractRangeQueryNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        FieldValuePairQueryNode<?> lowerBound = node.getLowerBound();
        FieldValuePairQueryNode<?> upperBound = node.getUpperBound();
        if (node.isLowerInclusive()) {
            sb.append("[");
        } else {
            sb.append("{");
        }
        if (lowerBound != null) {
            visit(lowerBound, sb);
        } else {
            sb.append("...");
        }
        sb.append(' ');
        if (upperBound != null) {
            visit(upperBound, sb);
        } else {
            sb.append("...");
        }
        if (node.isUpperInclusive()) {
            sb.append("]");
        } else {
            sb.append("}");
        }
        return sb;
    }

    @Override
    public Object visit(BooleanModifierNode node, Object data) {
        // BooleanModifierNode does not override toQueryString(). Default to behavior of parent class ModifierQueryNode.
        return visit((ModifierQueryNode) node, data);
    }

    @Override
    public Object visit(MultiPhraseQueryNode node, Object data) {
        List<QueryNode> children = node.getChildren();
        if (children != null && !children.isEmpty()) {
            StringBuilder sb = (StringBuilder) data;
            sb.append("[MTP[");
            sb.append(joinChildren(node, ","));
            sb.append("]]");
        }
        return data;
    }

    @Override
    public Object visit(PointQueryNode node, Object data) {
        String field = node.getField().toString();
        StringBuilder sb = (StringBuilder) data;
        boolean isDefaultField = LuceneQueryNodeHelper.isDefaultField(node, node.getField());
        if (!isDefaultField) {
            sb.append(field).append(":");
        }
        sb.append(escape(node.getNumberFormat().format(node.getValue()), Locale.ROOT, EscapeQuerySyntax.Type.NORMAL));
        return sb;
    }

    @Override
    public Object visit(PointRangeQueryNode node, Object data) {
        // PointRangeQueryNode does not override toQueryString(). Default to behavior of parent class AbstractRangeQueryNode.
        return visit((AbstractRangeQueryNode) node, data);
    }

    @Override
    public Object visit(PrefixWildcardQueryNode node, Object data) {
        // PrefixWildcardQueryNode does not override toQueryString(). Default to behavior of parent class WildcardQueryNode.
        return visit((WildcardQueryNode) node, data);
    }

    @Override
    public Object visit(RegexpQueryNode node, Object data) {
        String field = node.getField().toString();
        StringBuilder sb = (StringBuilder) data;
        boolean isDefaultField = LuceneQueryNodeHelper.isDefaultField(node, node.getField());
        if (!isDefaultField) {
            sb.append(field).append(":");
        }
        sb.append("/").append(node.getText()).append("/");
        return sb;
    }

    @Override
    public Object visit(SynonymQueryNode node, Object data) {
        // SynonymQueryNode does not override toQueryString(). Default to behavior of parent class BooleanQueryNode.
        return visit((BooleanQueryNode) node, data);
    }

    @Override
    public Object visit(TermRangeQueryNode node, Object data) {
        // TermRangeQueryNode does not override toQueryString(). Default to behavior of parent class AbstractRangeQueryNode.
        return visit((AbstractRangeQueryNode) node, data);
    }

    @Override
    public Object visit(WildcardQueryNode node, Object data) {
        String field = node.getField().toString();
        StringBuilder sb = (StringBuilder) data;
        boolean isDefaultField = LuceneQueryNodeHelper.isDefaultField(node, node.getField());
        if (!isDefaultField) {
            sb.append(field).append(":");
        }
        sb.append(node.getText());
        return sb;
    }

    @Override
    public Object visit(FunctionQueryNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("#");
        sb.append(node.getFunction());
        sb.append("(");
        String filler = "";
        for (String parameter : node.getParameterList()) {
            sb.append(filler).append(escape(parameter, Locale.getDefault(), EscapeQuerySyntax.Type.NORMAL));
            filler = ", ";
        }
        sb.append(")");
        return sb;
    }

    @Override
    public Object visit(NotBooleanQueryNode node, Object data) {
        // NotBooleanQueryNode does not override toQueryString(). Default to behavior of parent class BooleanQueryNode.
        return visit((BooleanQueryNode) node, data);
    }

    private Object visitJunctionNode(QueryNode node, Object data, String junction) {
        StringBuilder sb = (StringBuilder) data;
        List<QueryNode> children = node.getChildren();
        if (children != null && !children.isEmpty()) {
            boolean requiresGrouping = !isRootOrHasParentGroup(node);
            if (requiresGrouping) {
                sb.append("( ");
            }
            sb.append(joinChildren(node, junction));
            if (requiresGrouping) {
                sb.append(" )");
            }
        }
        return sb;
    }

    private String joinChildren(QueryNode node, String junction) {
        List<QueryNode> children = node.getChildren();
        if (children != null && !children.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            String filler = "";
            for (QueryNode child : children) {
                sb.append(filler);
                visit(child, sb);
                filler = junction;
            }
            return sb.toString();
        } else {
            return "";
        }
    }

    private boolean isRootOrHasParentGroup(QueryNode node) {
        QueryNode parent = node.getParent();
        return parent == null || parent instanceof GroupQueryNode;
    }

    private CharSequence escape(CharSequence text, Locale locale, EscapeQuerySyntax.Type type) {
        return escapedSyntax.escape(text, locale, type);
    }

    private String getFloatStr(Float floatValue) {
        if (floatValue == floatValue.longValue()) {
            return "" + floatValue.longValue();
        } else {
            return "" + floatValue;
        }
    }
}
