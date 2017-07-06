package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;

import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Fetch all regular expression patterns from the AST
 * 
 */
@SuppressWarnings("unchecked")
public class PatternNodeVisitor extends BaseVisitor {
    
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
}
