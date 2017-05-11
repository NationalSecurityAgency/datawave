package datawave.query.rewrite.jexl.visitors;

import java.util.Set;

import datawave.query.rewrite.jexl.JexlASTHelper;

import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Collect a {@link Set} of all literals/terms in the AST
 * 
 */
@SuppressWarnings("unchecked")
public class LiteralNodeVisitor extends BaseVisitor {
    
    public static Multimap<String,String> getLiterals(ASTJexlScript script) {
        LiteralNodeVisitor visitor = new LiteralNodeVisitor();
        
        return (Multimap<String,String>) script.jjtAccept(visitor, HashMultimap.create());
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        Multimap<String,String> literals = (Multimap<String,String>) data;
        
        String identifier = JexlASTHelper.getIdentifier(node);
        Object literal = JexlASTHelper.getLiteralValue(node);
        
        literals.put(identifier, (literal == null ? null : literal.toString()));
        
        return literals;
    }
    
}
