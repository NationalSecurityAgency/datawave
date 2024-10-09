package datawave.query.jexl.visitors;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang3.tuple.Pair;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;

/**
 * A visitor that fetches all fields found in the query that have a numeric value.
 */
public class FieldsWithNumericValuesVisitor extends ShortCircuitBaseVisitor {

    /**
     * Fetch all fields that have a numeric value.
     *
     * @param query
     *            the query
     * @return the set of fields
     */
    public static Set<String> getFields(ASTJexlScript query) {
        if (query == null) {
            return Collections.emptySet();
        } else {
            FieldsWithNumericValuesVisitor visitor = new FieldsWithNumericValuesVisitor();
            // Maintain insertion order of fields found.
            return (Set<String>) query.jjtAccept(visitor, new LinkedHashSet<String>());
        }
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        checkSingleField(node, data);
        return data;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        checkSingleField(node, data);
        return data;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        checkSingleField(node, data);
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        checkSingleField(node, data);
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        checkSingleField(node, data);
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        checkSingleField(node, data);
        return data;
    }

    private void checkSingleField(JexlNode node, Object data) {
        String field = JexlASTHelper.getIdentifier(node);
        if (field != null) {
            Object literal = JexlASTHelper.getLiteralValue(node);
            if (literal instanceof Number) {
                ((Set<String>) data).add(field);
            }
        }
    }
}
