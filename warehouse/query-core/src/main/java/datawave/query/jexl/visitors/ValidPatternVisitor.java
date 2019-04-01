package datawave.query.jexl.visitors;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import datawave.query.jexl.JexlASTHelper;

import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;

import com.google.common.collect.Maps;

/**
 * Validates all patterns in a query tree. Uses a cache to avoid parsing the same pattern twice.
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
    
    /**
     * Visit a Regex Equals node, catches the situation where a users might enter FIELD1 =~ FIELD2.
     *
     * @param node
     *            - an AST Regex Equals node
     * @param data
     *            - data
     * @return the data
     */
    @Override
    public Object visit(ASTERNode node, Object data) {
        Object literalValue;
        
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
    
    /**
     * Visit a Regex Not Equals node, catches the situation where a user might enter FIELD1 !~ FIELD2
     * 
     * @param node
     *            - an AST Regex Not Equals node
     * @param data
     *            - data
     * @return the data
     */
    @Override
    public Object visit(ASTNRNode node, Object data) {
        Object literalValue;
        
        // Catch the situation where a user might enter FIELD1 !~ FIELD2
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
