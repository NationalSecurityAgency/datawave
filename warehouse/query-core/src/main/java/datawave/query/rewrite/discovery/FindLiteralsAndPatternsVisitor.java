package datawave.query.rewrite.discovery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import datawave.query.rewrite.jexl.JexlASTHelper;
import datawave.query.rewrite.jexl.LiteralRange;
import datawave.query.rewrite.jexl.visitors.BaseVisitor;
import datawave.query.rewrite.jexl.visitors.RangeCoalescingVisitor;
import datawave.query.rewrite.jexl.visitors.TreeFlatteningRebuildingVisitor;

import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class FindLiteralsAndPatternsVisitor extends BaseVisitor {
    
    private static final Logger log = Logger.getLogger(FindLiteralsAndPatternsVisitor.class);
    
    private final QueryValues values = new QueryValues();
    
    private FindLiteralsAndPatternsVisitor() {}
    
    /**
     * Returns a pair containing the set of literals and the set of patterns in this script.
     * 
     * @param root
     * @return
     */
    public static QueryValues find(JexlNode root) {
        root = TreeFlatteningRebuildingVisitor.flatten(root);
        root = RangeCoalescingVisitor.coalesceRanges(root);
        FindLiteralsAndPatternsVisitor vis = new FindLiteralsAndPatternsVisitor();
        root.jjtAccept(vis, null);
        return vis.values;
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        // strings are always wrapped in a reference node, so the op is the gparent
        JexlNode op = node.jjtGetParent().jjtGetParent();
        if (op instanceof ASTEQNode) {
            JexlNode field = JexlNodes.otherChild(op, node.jjtGetParent()).jjtGetChild(0);
            if (log.isTraceEnabled()) {
                log.trace("Found field " + JexlASTHelper.deconstructIdentifier(field.image) + "==" + node.getLiteral().toString());
            }
            values.addLiteral(node.getLiteral().toString(), JexlASTHelper.deconstructIdentifier(field.image));
        } else if (op instanceof ASTERNode) {
            JexlNode field = JexlNodes.otherChild(op, node.jjtGetParent()).jjtGetChild(0);
            values.addPattern(node.getLiteral(), JexlASTHelper.deconstructIdentifier(field.image));
        }
        return null;
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        JexlNode op = node.jjtGetParent();
        if (op instanceof ASTEQNode) {
            JexlNode field = JexlNodes.otherChild(op, node).jjtGetChild(0);
            values.addLiteral(node.getLiteral().toString(), JexlASTHelper.deconstructIdentifier(field.image));
        }
        return null;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        List<JexlNode> otherNodes = new ArrayList<JexlNode>();
        Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRangesIndexAgnostic(node, otherNodes, true);
        if (ranges.size() == 1 && otherNodes.isEmpty()) {
            LiteralRange<?> range = ranges.keySet().iterator().next();
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
