package datawave.query.jexl.visitors;

import java.util.Set;

import datawave.query.jexl.JexlASTHelper;

import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Collect a {@link Set} of all literals/terms in the AST that belong to the specified set of fields.
 * 
 */
@SuppressWarnings("unchecked")
public class LiteralNodeSubsetVisitor extends BaseVisitor {
    
    public static Multimap<String,String> getLiterals(Set<String> expectedFields, ASTJexlScript script) {
        LiteralNodeSubsetVisitor visitor = new LiteralNodeSubsetVisitor(expectedFields);
        
        return (Multimap<String,String>) script.jjtAccept(visitor, HashMultimap.create());
    }
    
    protected final Set<String> expectedFields;
    
    public LiteralNodeSubsetVisitor(Set<String> expectedFields) {
        this.expectedFields = expectedFields;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        Multimap<String,String> literals = (Multimap<String,String>) data;
        
        String identifier = JexlASTHelper.getIdentifier(node);
        if (expectedFields.contains(identifier)) {
            Object literal = JexlASTHelper.getLiteralValue(node);
            literals.put(identifier, (literal == null ? null : literal.toString()));
        }
        
        return literals;
    }
    
}
