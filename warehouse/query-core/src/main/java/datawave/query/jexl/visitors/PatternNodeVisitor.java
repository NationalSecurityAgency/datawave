package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;

/**
 * Fetch all regular expression patterns from the AST
 */
@SuppressWarnings("unchecked")
public class PatternNodeVisitor extends ShortCircuitBaseVisitor {

    public static Multimap<String,String> getPatterns(ASTJexlScript script) {
        PatternNodeVisitor visitor = new PatternNodeVisitor();

        return (Multimap<String,String>) script.jjtAccept(visitor, HashMultimap.create());
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        Multimap<String,String> patterns = (Multimap<String,String>) data;

        String identifier = JexlASTHelper.getIdentifier(node);

        // The literal happens to be a pattern when we have a RegularExpression
        Object pattern = JexlASTHelper.getLiteralValue(node);

        patterns.put(identifier, pattern.toString());

        return patterns;
    }

    // Ensure we short circuit these nodes
    public Object visit(ASTLTNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return data;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return data;
    }

}
