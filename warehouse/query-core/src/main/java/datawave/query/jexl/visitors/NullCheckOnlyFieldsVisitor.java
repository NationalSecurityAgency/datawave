package datawave.query.jexl.visitors;

import java.util.Collection;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.JexlNode;

import com.google.common.base.Preconditions;

import datawave.query.jexl.JexlASTHelper;

/**
 * Finds the fields that a query references only in null checks, i.e. {@code FIELD == null}, {@code FIELD != null} or {@code !(FIELD == null)}. A field that is
 * referenced anywhere else in the query, such as in a value comparison, regex, or function, is not returned.
 */
public class NullCheckOnlyFieldsVisitor extends BaseVisitor {

    private final Set<String> nullCheckFields = new HashSet<>();
    private final Set<String> otherFields = new HashSet<>();

    /**
     * Get the fields that the query references only in null checks that are also in the given collection of fields of interest.
     *
     * @param node
     *            a query
     * @param fieldsOfInterest
     *            only fields in this collection that the query references only in null checks will be returned. No filtering will occur if the given collection
     *            is null or empty
     * @return the fields referenced only in null checks
     */
    public static Set<String> getFields(JexlNode node, Collection<String> fieldsOfInterest) {
        Set<String> fields = getFields(node);
        if (fieldsOfInterest != null && !fieldsOfInterest.isEmpty() && !fields.isEmpty()) {
            fields.retainAll(fieldsOfInterest);
        }
        return fields;
    }

    /**
     * Get the fields that the query references only in null checks.
     *
     * @param node
     *            a query
     * @return the fields referenced only in null checks
     * @throws NullPointerException
     *             if the given query node is null
     */
    public static Set<String> getFields(JexlNode node) {
        Preconditions.checkNotNull(node, "node must not be null");
        NullCheckOnlyFieldsVisitor visitor = new NullCheckOnlyFieldsVisitor();
        node.jjtAccept(visitor, null);
        Set<String> fields = new HashSet<>(visitor.nullCheckFields);
        fields.removeAll(visitor.otherFields);
        return fields;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return visitComparison(node, data);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return visitComparison(node, data);
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
        otherFields.add(JexlASTHelper.deconstructIdentifier(node));
        return data;
    }

    private Object visitComparison(JexlNode node, Object data) {
        if (isNullLiteralComparison(node)) {
            for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(node)) {
                nullCheckFields.add(JexlASTHelper.deconstructIdentifier(identifier));
            }
            return data;
        }
        node.childrenAccept(this, data);
        return data;
    }

    private boolean isNullLiteralComparison(JexlNode node) {
        try {
            return JexlASTHelper.getLiteral(node) instanceof ASTNullLiteral;
        } catch (NoSuchElementException e) {
            return false;
        }
    }
}
