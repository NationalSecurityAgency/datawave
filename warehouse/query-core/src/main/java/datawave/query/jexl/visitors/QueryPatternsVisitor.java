package datawave.query.jexl.visitors;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;

/**
 * A visitor that will return the set of all unique patterns found in the query.
 */
public class QueryPatternsVisitor extends ShortCircuitBaseVisitor {

    /**
     * Return the set of all patterns found in the query.
     *
     * @param query
     *            the query
     * @return the patterns
     */
    public static Set<String> findPatterns(ASTJexlScript query) {
        Set<String> patterns = new HashSet<>();
        if (query == null) {
            return patterns;
        } else {
            QueryPatternsVisitor visitor = new QueryPatternsVisitor();
            query.jjtAccept(visitor, patterns);
            return patterns;
        }
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        addPattern(node, data);
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        addPattern(node, data);
        return data;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        JexlArgumentDescriptor descriptor = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        if (descriptor == null) {
            throw new IllegalStateException("Could not get descriptor for ASTFunctionNode");
        }

        // If the function descriptor indicates the function has regex arguments, extract the arguments.
        if (descriptor.regexArguments()) {
            FunctionJexlNodeVisitor functionVisitor = new FunctionJexlNodeVisitor();
            functionVisitor.visit(node, null);
            List<JexlNode> args = functionVisitor.args();
            // Add each string literal argument as a regex pattern.
            args.stream().filter(arg -> arg instanceof ASTStringLiteral).forEach(arg -> addPattern(arg, data));
        }
        return data;
    }

    private void addPattern(JexlNode node, Object data) {
        // Catch the situation where a user might enter FIELD1 !~ VALUE1
        Object literalValue;
        try {
            literalValue = JexlASTHelper.getLiteralValue(node);
        } catch (Exception e) {
            // in this case there was no literal (e.g. FIELD1 !~ FIELD2)
            return;
        }

        if (literalValue != null && String.class.equals(literalValue.getClass())) {
            // noinspection unchecked
            ((Set<String>) data).add((String) literalValue);
        }
    }
}
