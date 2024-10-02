package datawave.query.jexl.visitors;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.NodeTypeCount;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.pushdown.AnchorDetectionVisitor;

/**
 * Rewrites regex terms as filter functions provided an anchor exists.
 * <p>
 * An anchor is an executable term or subtree.
 * <p>
 * This visitor supports several configuration options
 * <p>
 * <b>IncludeFields</b>
 * <p>
 * Limit rewrite operations to the specified fields
 * </p>
 * <p>
 * <b>ExcludeFields</b>
 * <p>
 * Rewrite operations will not be applied to the specified fields. This option overrides any 'include fields' but can be superseded by
 * {@link RegexRewritePattern}
 * </p>
 * <p>
 * <b>RegexRewritePattern</b>
 * <p>
 * In very specific cases one may want to always attempt a regex rewrite, regardless of any previously specified include or exclude fields
 * </p>
 */
public class RewriteRegexVisitor extends ShortCircuitBaseVisitor {

    private final Set<String> indexedFields;
    private final Set<String> indexOnlyFields;

    private final Set<String> includeFields;
    private final Set<String> excludeFields;

    private final Set<RegexRewritePattern> patterns;

    private final AnchorDetectionVisitor anchorDetectionVisitor;

    /**
     * Constructor with minimal args
     *
     * @param indexedFields
     *            the set of indexed fields
     * @param indexOnlyFields
     *            the set of index only fields
     */
    public RewriteRegexVisitor(Set<String> indexedFields, Set<String> indexOnlyFields) {
        this(indexedFields, indexOnlyFields, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    }

    /**
     * Constructor with minimal args
     *
     * @param indexedFields
     *            the set of indexed fields
     * @param indexOnlyFields
     *            the set of index only fields
     */
    public RewriteRegexVisitor(Set<String> indexedFields, Set<String> indexOnlyFields, Set<String> includeFields, Set<String> excludeFields,
                    Set<RegexRewritePattern> patterns) {
        this.indexedFields = indexedFields;
        this.indexOnlyFields = indexOnlyFields;
        this.includeFields = includeFields;
        this.excludeFields = excludeFields;
        this.patterns = patterns;

        this.anchorDetectionVisitor = new AnchorDetectionVisitor(indexedFields, indexOnlyFields);
    }

    /**
     * Static entry point
     *
     * @param node
     *            the query or subtree
     * @param indexedFields
     *            the set of indexed fields
     * @param indexOnlyFields
     *            the set of index only fields
     * @return the modified tree
     */
    public static JexlNode rewrite(JexlNode node, Set<String> indexedFields, Set<String> indexOnlyFields) {
        return rewrite(node, indexedFields, indexOnlyFields, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    }

    public static JexlNode rewrite(JexlNode node, Set<String> indexedFields, Set<String> indexOnlyFields, Set<String> includeFields, Set<String> excludeFields,
                    Set<RegexRewritePattern> patterns) {
        RewriteRegexVisitor visitor = new RewriteRegexVisitor(indexedFields, indexOnlyFields, includeFields, excludeFields, patterns);
        node.jjtAccept(visitor, null);
        return node;
    }

    // union is not overridden here

    @Override
    public Object visit(ASTAndNode node, Object data) {

        if (data instanceof Boolean) {
            return data; // short circuit repeated post-traversals
        }

        if (QueryPropertyMarker.findInstance(node).isAnyType()) {
            return data; // do not descend into markers
        }

        // enforce a post-order traversal for maximum rewrite
        node.childrenAccept(this, data);

        List<JexlNode> anchorCandidates = new LinkedList<>();
        List<JexlNode> anchorNonCandidates = new LinkedList<>();
        List<JexlNode> otherCandidates = new LinkedList<>();

        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);

            // this seems expensive, a visitor that returned raw counts, depth, and complexity would nice to have
            NodeTypeCount counts = NodeTypeCountVisitor.countNodes(child, ASTERNode.class);

            if (anchorDetectionVisitor.isAnchor(child)) {
                if (counts.getTotal(ASTERNode.class) > 0) {
                    anchorCandidates.add(child);
                } else {
                    anchorNonCandidates.add(child);
                }
            } else if (counts.getTotal(ASTERNode.class) > 0) {
                otherCandidates.add(child);
            }
        }

        if (!anchorCandidates.isEmpty() || !anchorNonCandidates.isEmpty()) {

            if (!anchorNonCandidates.isEmpty()) {
                // rewrite all anchor candidates
                for (JexlNode candidate : anchorCandidates) {
                    candidate.jjtAccept(this, true);
                }
            } else {
                // rewrite all anchor candidates except the last one, to preserve executability
                for (int i = 0; i < anchorCandidates.size() - 1; i++) {
                    anchorCandidates.get(i).jjtAccept(this, true);
                }
            }

            // if any anchor exists, rewrite other candidates
            for (JexlNode otherCandidate : otherCandidates) {
                otherCandidate.jjtAccept(this, true);
            }
        }

        return data;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        String field = JexlASTHelper.getIdentifier(node);

        if (isLegalRewrite(field, data)) {

            // once legality of rewrite is established make sure it's not filtered
            String literal = (String) JexlASTHelper.getLiteralValue(node);

            if (isNodeRewritableFromRules(field, literal)) {
                JexlNode rewrite = JexlNodeFactory.buildFunctionNode("filter", "includeRegex", field, literal);
                JexlNodes.replaceChild(node.jjtGetParent(), node, rewrite);
            }
        }

        return data;
    }

    private boolean isLegalRewrite(String field, Object data) {
        // never rewrite ANY_FIELD or index-only fields
        if (field.equals(Constants.ANY_FIELD) || indexOnlyFields.contains(field)) {
            return false;
        }

        // 1. anchor exists elsewhere
        // 2. field is not indexed
        return data instanceof Boolean || !indexedFields.contains(field);
    }

    /**
     * Determine if the node can be rewritten given any configured rules (include fields, exclude fields, patterns)
     *
     * @param field
     *            the field
     * @param literal
     *            the literal
     * @return true if the node can be rewritten
     */
    private boolean isNodeRewritableFromRules(String field, String literal) {
        // check patterns first because they supersede include/exclude rules
        for (RegexRewritePattern pattern : patterns) {
            if (pattern.matches(field, literal)) {
                return true;
            }
        }

        // exclude fields beat include fields
        if (!excludeFields.isEmpty() && excludeFields.contains(field)) {
            return false;
        }

        if (!includeFields.isEmpty()) {
            return includeFields.contains(field);
        }

        return true;
    }
}
