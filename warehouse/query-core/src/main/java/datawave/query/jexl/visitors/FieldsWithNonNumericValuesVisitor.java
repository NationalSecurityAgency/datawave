package datawave.query.jexl.visitors;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang3.math.NumberUtils;

import datawave.query.jexl.JexlASTHelper;

/**
 * Visitor that collects fields whose comparison literal is a string that cannot be parsed as a number.
 * <p>
 * All simple comparison operators are inspected (equality, inequality, and the four range operators).
 * Null and numeric literals are ignored. This is the inverse of {@link FieldsWithNumericValuesVisitor};
 * see also {@link FieldsWithNumericRangeVisitor} for the range only numeric case.
 */
public class FieldsWithNonNumericValuesVisitor extends ShortCircuitBaseVisitor {

    /**
     * Fetch all fields that are compared against a non-numeric string literal.
     * 
     * @param query
     *            The parsed JEXL query script
     * @return ordered set of field names
     */
    @SuppressWarnings("unchecked")
    public static Set<String> getFields(ASTJexlScript query) {
        if (query == null) {
            return Collections.emptySet();
        } else {
            FieldsWithNonNumericValuesVisitor visitor = new FieldsWithNonNumericValuesVisitor();
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

    @SuppressWarnings("unchecked")
    private void checkSingleField(JexlNode node, Object data) {
        String field = JexlASTHelper.getIdentifier(node);
        if (field != null) {
            Object literal = JexlASTHelper.getLiteralValueSafely(node);
            if (literal instanceof String && !NumberUtils.isCreatable((String) literal)) {
                ((Set<String>) data).add(field);
            }
        }
    }
}