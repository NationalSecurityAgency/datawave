package datawave.query.jexl.visitors;

import com.google.common.collect.Maps;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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
     * Visit a Regex Equals node, catches the situation where a users might enter FIELD1 =~ VALUE1.
     *
     * @param node
     *            - an AST Regex Equals node
     * @param data
     *            - data
     * @return the data
     */
    @Override
    public Object visit(ASTERNode node, Object data) {
        parseAndPutLiteral(node);
        return data;
    }
    
    /**
     * Visit a Regex Not Equals node, catches the situation where a user might enter FIELD1 !~ VALUE1
     * 
     * @param node
     *            - an AST Regex Not Equals node
     * @param data
     *            - data
     * @return the data
     */
    @Override
    public Object visit(ASTNRNode node, Object data) {
        parseAndPutLiteral(node);
        return data;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        
        if (node.jjtGetNumChildren() >= 4) {
            
            // Should be a filter function.
            String functionType = JexlASTHelper.getIdentifier(node.jjtGetChild(0));
            if (functionType == null || !functionType.equals("filter")) {
                return data;
            }
            
            // Filter should be includeRegex or excludeRegex.
            String filterType = JexlASTHelper.getIdentifier(node.jjtGetChild(1));
            if (filterType == null || !(filterType.equals("includeRegex") || filterType.equals("excludeRegex"))) {
                return data;
            }
            
            // Child 3 is the field to be included/excluded.
            // JexlNode child3 = node.jjtGetChild(2);
            
            // Child 4 is the field that *may* contain a regex.
            JexlNode child4 = node.jjtGetChild(3);
            if (child4 instanceof ASTReference) {
                
                JexlNode literalNode = child4.jjtGetChild(0);
                if (StringUtils.containsAny(literalNode.image, "?.*^+-[]()")) {
                    try {
                        parseAndPutLiteral(literalNode);
                    } catch (PatternSyntaxException e) {
                        String builtNode = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
                        String errMsg = "Invalid pattern found in filter function '" + builtNode + "'";
                        throw new PatternSyntaxException(errMsg, e.getPattern(), e.getIndex());
                    }
                }
            }
        }
        
        // Do not descend to children, the ValidPatternVisitor views a function node as a leaf node.
        return data;
    }
    
    /**
     * Parse a literal value and put into the pattern cache if it does not exist.
     * 
     * @param node
     */
    public void parseAndPutLiteral(JexlNode node) {
        Object literalValue;
        
        // Catch the situation where a user might enter FIELD1 !~ VALUE1
        try {
            literalValue = JexlASTHelper.getLiteralValue(node);
        } catch (NoSuchElementException e) {
            return;
        }
        
        if (literalValue != null && String.class.equals(literalValue.getClass())) {
            String literalString = (String) literalValue;
            if (patternCache.containsKey(literalString)) {
                return;
            }
            patternCache.put(literalString, Pattern.compile(literalString));
        }
        return;
    }
}
