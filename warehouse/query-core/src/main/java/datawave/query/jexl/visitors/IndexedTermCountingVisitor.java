package datawave.query.jexl.visitors;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.jexl.JexlASTHelper;

/**
 * Helper visitor that counts the number of EQ nodes with an indexed field. Used to limit large queries prior to the range stream lookup.
 */
public class IndexedTermCountingVisitor extends ShortCircuitBaseVisitor {

    private final Set<String> indexedFields;
    private int count = 0;

    /**
     * Counts the number of EQ nodes with an indexed field
     *
     * @param node
     *            the query tree
     * @param indexedFields
     *            the set of indexed fields
     * @return the number of indexed EQ nodes
     */
    public static int countTerms(JexlNode node, Set<String> indexedFields) {
        IndexedTermCountingVisitor visitor = new IndexedTermCountingVisitor(indexedFields);
        node.jjtAccept(visitor, null);
        return visitor.getCount();
    }

    /**
     * Constructor that accepts a set of indexed fields
     *
     * @param indexedFields
     *            a set of indexed fields
     */
    public IndexedTermCountingVisitor(Set<String> indexedFields) {
        this.indexedFields = indexedFields;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        String field = JexlASTHelper.getIdentifier(node);
        if (indexedFields.contains(field)) {
            count++;
        }
        return data;
    }

    public int getCount() {
        return count;
    }
}
