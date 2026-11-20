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
 * Finds all fields that a query references in null checks, i.e. {@code FIELD == null}, {@code FIELD != null} or {@code !(FIELD == null)}. A field is returned
 * even if it is also referenced elsewhere in the query, such as in a value comparison, regex, or function.
 */
public class NullCheckFieldsVisitor extends BaseVisitor {

    private final Set<String> nullCheckFields = new HashSet<>();

    /**
     * Get the fields that the query references in null checks that are also in the given collection of fields of interest, regardless of whether the query
     * references them elsewhere.
     *
     * @param node
     *            a query
     * @param fieldsOfInterest
     *            only fields in this collection will be returned. No filtering will occur if the given collection is null or empty
     * @return the fields referenced in null checks
     * @throws NullPointerException
     *             if the given query node is null
     */
    public static Set<String> getFields(JexlNode node, Collection<String> fieldsOfInterest) {
        Preconditions.checkNotNull(node, "node must not be null");
        NullCheckFieldsVisitor visitor = new NullCheckFieldsVisitor();
        node.jjtAccept(visitor, null);
        if (fieldsOfInterest != null && !fieldsOfInterest.isEmpty() && !visitor.nullCheckFields.isEmpty()) {
            visitor.nullCheckFields.retainAll(fieldsOfInterest);
        }
        return visitor.nullCheckFields;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return visitComparison(node, data);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return visitComparison(node, data);
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
