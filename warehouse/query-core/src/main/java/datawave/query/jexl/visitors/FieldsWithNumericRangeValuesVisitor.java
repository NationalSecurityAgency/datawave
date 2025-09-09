package datawave.query.jexl.visitors;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang3.math.NumberUtils;

import datawave.query.jexl.JexlASTHelper;

/**
 * Visitor that collects fields that participate in numeric range comparisons.
 * <p>
 * This visitor inspects inequality comparison nodes (less-than, less-than-or-equal, greater-than, greater-than-or-equal) and returns the ordered set of field
 * names whose compared literal is numeric, or a string value that can be parsed as a number. Equality and not-equal comparisons are intentionally ignored. For
 * collecting all numeric comparisons (range and equality), see {@link FieldsWithNumericValuesVisitor}.
 * </p>
 */
public class FieldsWithNumericRangeValuesVisitor extends ShortCircuitBaseVisitor {

    /**
     * Collect the set of field names that appear in comparison nodes that represent ranges.
     *
     * @param query
     *            the parsed JEXL query script
     * @return ordered set of field names encountered in range comparisons
     */
    @SuppressWarnings("unchecked")
    public static Set<String> getFields(ASTJexlScript query) {
        if (query == null) {
            return Collections.emptySet();
        } else {
            FieldsWithNumericRangeValuesVisitor visitor = new FieldsWithNumericRangeValuesVisitor();
            return (Set<String>) query.jjtAccept(visitor, new LinkedHashSet<String>());
        }
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        checkComparisonNodeField(node, data);
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        checkComparisonNodeField(node, data);
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        checkComparisonNodeField(node, data);
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        checkComparisonNodeField(node, data);
        return data;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        return super.visit(node, data);
    }

    /**
     * If the supplied node is a comparison node, add its field identifier to the output set.
     *
     * @param node
     *            a JEXL comparison node
     * @param data
     *            the accumulating set {@code (LinkedHashSet<String>)}
     */
    @SuppressWarnings("unchecked")
    private void checkComparisonNodeField(JexlNode node, Object data) {
        String field = JexlASTHelper.getIdentifier(node);
        if (field != null) {
            Object literal = JexlASTHelper.getLiteralValueSafely(node);
            if (literal instanceof Number) {
                // Only track fields that compare against numeric values.
                ((Set<String>) data).add(field);
            } else if (literal instanceof String) {
                if (NumberUtils.isCreatable((String) literal)) {
                    ((Set<String>) data).add(field);
                }
            }
        }
    }
}
