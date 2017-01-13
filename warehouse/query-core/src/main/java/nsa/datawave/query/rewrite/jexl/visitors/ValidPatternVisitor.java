package nsa.datawave.query.rewrite.jexl.visitors;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import nsa.datawave.query.rewrite.jexl.JexlASTHelper;

import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;

import com.google.common.collect.Maps;

/**
 * 
 */
public class ValidPatternVisitor extends BaseVisitor {
    
    private Map<String,Pattern> patternCache;
    
    public ValidPatternVisitor() {
        patternCache = Maps.newHashMap();
    }
    
    public static void check(JexlNode node) {
        ValidPatternVisitor visitor = new ValidPatternVisitor();
        
        node.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        Object literalValue = null;
        
        // Catch the situation where a user might enter FIELD1 =~ FIELD2
        try {
            literalValue = JexlASTHelper.getLiteralValue(node);
        } catch (NoSuchElementException e) {
            return data;
        }
        
        if (String.class.equals(literalValue.getClass())) {
            String literalString = (String) literalValue;
            if (patternCache.containsKey(literalString)) {
                return data;
            }
            
            patternCache.put(literalString, Pattern.compile(literalString));
        }
        
        return data;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        Object literalValue = null;
        
        // Catch the situation where a user might enter FIELD1 =~ FIELD2
        try {
            literalValue = JexlASTHelper.getLiteralValue(node);
        } catch (NoSuchElementException e) {
            return data;
        }
        
        if (String.class.equals(literalValue.getClass())) {
            String literalString = (String) literalValue;
            if (patternCache.containsKey(literalString)) {
                return data;
            }
            
            patternCache.put(literalString, Pattern.compile(literalString));
        }
        
        return data;
    }
}
