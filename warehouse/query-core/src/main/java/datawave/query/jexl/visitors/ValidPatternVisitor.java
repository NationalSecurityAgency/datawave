package datawave.query.jexl.visitors;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.JexlNode;

import com.google.common.collect.Maps;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;

/**
 * Validates all patterns in a query tree. Uses a cache to avoid parsing the same pattern twice.
 */
public class ValidPatternVisitor extends ShortCircuitBaseVisitor {

    private Map<String,Pattern> patternCache;

    public ValidPatternVisitor() {
        this.patternCache = Maps.newHashMap();
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
     *            the node data
     * @return the data
     */
    @Override
    public Object visit(ASTERNode node, Object data) {
        parseAndPutPattern(node);
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
        parseAndPutPattern(node);
        return data;
    }

    /**
     * Visit an ASTFunctionNode to catch cases like #INCLUDE or #EXCLUDE that accept a regex as an argument
     *
     * @param node
     *            - an ASTFunctionNode
     * @param data
     *            - the data
     * @return the data
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {

        // Should pull back an EvaluationPhaseFilterFunctionsDescriptor
        JexlArgumentDescriptor descriptor = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        if (descriptor == null) {
            throw new IllegalStateException("Could not get descriptor for ASTFunctionNode");
        }

        if (descriptor.regexArguments()) {
            // Extract the args for this function
            FunctionJexlNodeVisitor functionVisitor = new FunctionJexlNodeVisitor();
            functionVisitor.visit(node, null);
            List<JexlNode> args = functionVisitor.args();
            for (JexlNode arg : args) {
                // Only take the literals
                if (arg instanceof ASTStringLiteral) {
                    parseAndPutPattern(arg);
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
     *            a jexl node
     */
    public void parseAndPutPattern(JexlNode node) {
        // Catch the situation where a user might enter FIELD1 !~ VALUE1
        Object literalValue;
        try {
            literalValue = JexlASTHelper.getLiteralValue(node);
        } catch (Exception e) {
            // in this case there was no literal (e.g. FIELD1 !~ FIELD2)
            return;
        }

        if (literalValue != null && String.class.equals(literalValue.getClass())) {
            String literalString = (String) literalValue;
            try {
                if (patternCache.containsKey(literalString)) {
                    return;
                }
                patternCache.put(literalString, Pattern.compile(literalString));
            } catch (PatternSyntaxException e) {
                String builtNode = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
                String errMsg = "Invalid pattern found in filter function '" + builtNode + "'";
                throw new PatternSyntaxException(errMsg, e.getPattern(), e.getIndex());
            }
        }
    }

}
