package datawave.query.transformer.annotation;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;

import datawave.data.normalizer.Normalizer;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.PushdownNegationVisitor;
import datawave.query.parser.JavaRegexAnalyzer;

public class TermExtractor implements Serializable {
    private Set<String> fields;

    public TermExtractor(Set<String> fields) {
        this.fields = fields;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TermExtractor)) {
            return false;
        }

        return Objects.equals(fields, ((TermExtractor) other).fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

    /**
     * Check if this node is acceptable. Node must already have negations pushed down so there will be 0 to 1 between it and the root. If it has a negation in
     * its lineage, or has no identifier, or its identifier is outside the accepted fields it will not be accepted
     *
     * @param node
     * @return true if the node is acceptable, false otherwise
     */
    private boolean acceptNode(JexlNode node) {
        JexlNode parent = node.jjtGetParent();
        // check if the node has a negation over it
        while (parent != null) {
            if (parent instanceof ASTNotNode) {
                return false;
            }
            parent = parent.jjtGetParent();
        }

        String identifier = JexlASTHelper.getIdentifier(node);
        if (identifier == null) {
            return false;
        }

        return fields == null || fields.contains(identifier);
    }

    /**
     * Extract Patterns of query terms and regexes from the jexl query. Only extract terms that are not negated. All Patterns will be normalized and
     * case-insensitive and unicode-case flagged.
     *
     * @param jexlQueryString
     *            the jexl parsed query before query planning
     * @param normalizer
     *            The term normalizer to use on extracted terms and patterns
     * @return
     * @throws ParseException
     */
    public Set<String> extract(String jexlQueryString, Normalizer<String> normalizer) throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        Set<String> terms = new HashSet<>();

        // parse the original query into jexl
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(jexlQueryString);
        // push all negations all the way to the leaf nodes of the jexl tree. PushdownNegationVisitor does not convert EQ -> NEQ or ER -> NR nodes, but it does
        // convert NE -> EQ and NR -> ER nodes. This guarantees there is at most one negation between any leaf and the root node.
        JexlNode fullyNegated = PushdownNegationVisitor.pushdownNegations(script);

        List<ASTEQNode> eqNodes = JexlASTHelper.getEQNodes(fullyNegated);
        if (!eqNodes.isEmpty()) {
            for (ASTEQNode eqNode : eqNodes) {
                if (acceptNode(eqNode)) {
                    Object literal = JexlASTHelper.getLiteralValue(eqNode);
                    if (literal != null) {
                        // simple normalization for exact string matches only
                        terms.add(normalizer.normalize(literal.toString()));
                    }
                }
            }

        }
        // handle limited cases of er nodes
        List<ASTERNode> erNodes = JexlASTHelper.getERNodes(fullyNegated);
        if (!erNodes.isEmpty()) {
            for (ASTERNode erNode : erNodes) {
                if (acceptNode(erNode)) {
                    Object literal = JexlASTHelper.getLiteralValue(erNode);
                    if (literal != null) {
                        terms.add(normalizer.normalizeRegex(literal.toString()));
                    }
                }
            }
        }

        return terms;
    }
}
