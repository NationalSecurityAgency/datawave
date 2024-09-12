package datawave.query.discovery;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;

public class FindLiteralsAndPatternsVisitor extends BaseVisitor {

    private static final Logger log = LogManager.getLogger(FindLiteralsAndPatternsVisitor.class);

    private final QueryValues values = new QueryValues();

    private FindLiteralsAndPatternsVisitor() {}

    /**
     * Returns a pair containing the set of literals and the set of patterns in this script.
     *
     * @param root
     *            the root node
     * @return a set of literals and patterns
     */
    public static QueryValues find(JexlNode root) {
        root = TreeFlatteningRebuildingVisitor.flatten(root);
        FindLiteralsAndPatternsVisitor vis = new FindLiteralsAndPatternsVisitor();
        root.jjtAccept(vis, null);
        return vis.values;
    }

    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        // strings are NEVER wrapped in a reference node, so the op is the parent
        JexlNode op = node.jjtGetParent();
        if (op instanceof ASTEQNode) {
            JexlNode field = JexlNodes.otherChild(op, node);
            if (log.isTraceEnabled()) {
                log.trace("Found field " + JexlASTHelper.deconstructIdentifier(JexlNodes.getIdentifierOrLiteralAsString(field)) + "==" + node.getLiteral());
            }
            values.addLiteral(node.getLiteral(), JexlASTHelper.deconstructIdentifier(JexlNodes.getIdentifierOrLiteralAsString(field)));
        } else if (op instanceof ASTERNode) {
            JexlNode field = JexlNodes.otherChild(op, node);
            values.addPattern(node.getLiteral(), JexlASTHelper.deconstructIdentifier(JexlNodes.getIdentifierOrLiteralAsString(field)));
        }
        return null;
    }

    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        JexlNode op = node.jjtGetParent();
        if (op instanceof ASTEQNode) {
            JexlNode field = JexlNodes.otherChild(op, node);
            values.addLiteral(node.getLiteral().toString(), JexlASTHelper.deconstructIdentifier(((ASTIdentifier) field).getName()));
        }
        return null;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        LiteralRange range = JexlASTHelper.findRange().getRange(node);
        if (range != null) {
            values.addRange(range.getFieldName(), range);
        } else {
            super.visit(node, data);
        }

        return null;
    }

    public static class QueryValues {
        private Multimap<String,String> literals = HashMultimap.create(), patterns = HashMultimap.create();
        private Multimap<String,LiteralRange<?>> ranges = HashMultimap.create();

        public Multimap<String,String> getLiterals() {
            return literals;
        }

        public void addLiteral(String field, String literal) {
            literals.put(field, literal);
        }

        public Multimap<String,String> getPatterns() {
            return patterns;
        }

        public void addPattern(String field, String pattern) {
            patterns.put(field, pattern);
        }

        public Multimap<String,LiteralRange<?>> getRanges() {
            return ranges;
        }

        public void addRange(String field, LiteralRange<?> range) {
            ranges.put(field, range);
        }
    }
}
