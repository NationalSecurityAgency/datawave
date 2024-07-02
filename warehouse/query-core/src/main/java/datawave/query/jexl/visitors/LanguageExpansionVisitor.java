package datawave.query.jexl.visitors;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

import com.google.common.base.Preconditions;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.language.analyzers.LanguageAnalyzer;
import datawave.query.language.analyzers.lucene.LanguageAwareAnalyzer;

/**
 * A visitor that expands query nodes based on plausible language-based alternates
 */
public class LanguageExpansionVisitor extends ShortCircuitBaseVisitor {

    private final int maxExpansion = 10;
    private boolean useLemmas = true;
    private boolean useStems = true;

    private List<LanguageAwareAnalyzer> analyzers;

    private Set<String> tokenizedFields;

    private LanguageExpansionVisitor() {
        // enforce using the parameterized constructor
    }

    /**
     * Default constructor
     *
     * @param analyzers
     *            a list of {@link LanguageAnalyzer}s
     * @param tokenizedFields
     *            a set of tokenized fields
     */
    public LanguageExpansionVisitor(List<LanguageAwareAnalyzer> analyzers, Set<String> tokenizedFields) {
        this.analyzers = analyzers;
        this.tokenizedFields = tokenizedFields;
    }

    /**
     * Static entry point for expanding an arbitrary JexlNode based on language analysis
     *
     * @param node
     *            a JexlNode
     * @param analyzers
     *            a list of {@link LanguageAnalyzer}s
     * @param tokenizedFields
     *            a set of tokenized fields
     * @return a potentially expanded query
     */
    public static JexlNode expand(JexlNode node, List<LanguageAwareAnalyzer> analyzers, Set<String> tokenizedFields) {
        LanguageExpansionVisitor visitor = new LanguageExpansionVisitor(analyzers, tokenizedFields);
        node.childrenAccept(visitor, null);
        return node;
    }

    /**
     * An entry point for expanding an arbitrary JexlNode based on language analysis. Useful for reusing the underlying visitor.
     *
     * @param node
     *            a JexlNode
     * @return a potentially expanded query
     */
    public JexlNode expand(JexlNode node) {
        Preconditions.checkNotNull(analyzers);
        Preconditions.checkNotNull(tokenizedFields);

        if (node instanceof ASTEQNode) {
            visit((ASTEQNode) node, null);
        } else {
            node.childrenAccept(this, null);
        }
        return node;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        String field = JexlASTHelper.getIdentifier(node);
        String value = (String) JexlASTHelper.getLiteralValueSafely(node);

        if (field != null && value != null && tokenizedFields.contains(field)) {

            Set<String> values = new HashSet<>();
            for (LanguageAwareAnalyzer analyzer : analyzers) {
                if (analyzer.matches(value)) {
                    values.addAll(analyzer.findAlternates(field, value));
                }
            }

            if (!values.isEmpty()) {
                expandNodeValues(field, values, node);
            }
        }

        return data;
    }

    /**
     * Expand an existing node into a junction based on alternate values
     *
     * @param field
     *            the field
     * @param values
     *            the set of alternate values
     * @param node
     *            the original node
     */
    private void expandNodeValues(String field, Set<String> values, JexlNode node) {
        JexlNode childOfJunction = findChildOfJunction(node);

        // build up the junction
        List<JexlNode> nodes = new ArrayList<>();
        for (String value : values) {
            nodes.add(JexlNodeFactory.buildEQNode(field, value));
        }

        // add a copy of the original node
        nodes.add(RebuildingVisitor.copy(node));

        ASTOrNode union = (ASTOrNode) JexlNodeFactory.createOrNode(nodes);

        // no guarantee as to how this visitor is being used, it might be handed a single node
        // with no parents

        if (childOfJunction == null) {
            if (node.jjtGetParent() != null) {
                JexlNodes.replaceChild(node.jjtGetParent(), node, union);
            }
        } else {
            JexlNode junction = childOfJunction.jjtGetParent();
            if (junction instanceof ASTAndNode) {
                JexlNodes.replaceChild(junction, childOfJunction, union);
            } else if (junction instanceof ASTOrNode) {
                integrateChildrenIntoUnion(junction, childOfJunction, union);
            }
        }
    }

    /**
     * Ascend the tree looking for the first parent that is a junction (And/Or node).
     *
     * @param node
     *            a JexlNode
     * @return the first junction parent, or null if no such parent exists
     */
    private JexlNode findChildOfJunction(JexlNode node) {
        JexlNode child = node;
        JexlNode parent = node.jjtGetParent();
        while (parent != null) {
            if (parent instanceof ASTAndNode || parent instanceof ASTOrNode) {
                return child;
            }

            child = parent;
            parent = child.jjtGetParent();
        }
        return null;
    }

    /**
     * Integrates the new union into a junction that is a union.
     *
     * @param junction
     *            a parent junction that is a union
     * @param childOfJunction
     *            the child that ultimately leads to our node
     * @param union
     *            the union of expanded equality nodes
     */
    private void integrateChildrenIntoUnion(JexlNode junction, JexlNode childOfJunction, JexlNode union) {
        // more complicated because we want to replace a node while preserving order
        // (A || B || C) becomes (A || B1 || B2 || C)
        JexlNode[] children = new JexlNode[junction.jjtGetNumChildren() + union.jjtGetNumChildren() - 1];

        int offset = 0;
        for (int i = 0; i < junction.jjtGetNumChildren(); i++) {
            JexlNode child = junction.jjtGetChild(i);
            if (child != childOfJunction) {
                children[i + offset] = child;
            } else {
                for (int j = 0; j < union.jjtGetNumChildren(); j++) {
                    offset = j;
                    children[i + j] = union.jjtGetChild(j);
                }
            }
        }

        JexlNodes.removeFromParent(junction, childOfJunction);
        JexlNodes.ensureCapacity(junction, junction.jjtGetNumChildren() + union.jjtGetNumChildren());
        JexlNodes.setChildren(junction, children);
    }

    /**
     * Set a list of {@link LanguageAnalyzer}s on this visitor
     *
     * @param analyzers
     *            a list of LanguageAnalyzers
     */
    public void setAnalyzers(List<LanguageAwareAnalyzer> analyzers) {
        this.analyzers = analyzers;
    }
}
