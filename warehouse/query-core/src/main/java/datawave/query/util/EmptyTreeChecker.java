package datawave.query.util;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;

/**
 * A utility class that contains logic for empty query tree checks. This utility should be used by visitors that modify the query tree
 * <p>
 * One use case is the {@link QueryOptionsFromQueryVisitor} which removes function nodes. If a query contains only function nodes the resulting query tree will
 * be empty.
 */
public class EmptyTreeChecker {

    private EmptyTreeChecker() {
        // enforce static access
    }

    /**
     * Check if the provided JexlNode is null or empty. The JexlNode should be the root of the query tree.
     *
     * @param node
     *            the root node
     */
    public static void check(JexlNode node) {
        if (node == null) {
            throw new DatawaveFatalQueryException("QueryTree was null");
        }

        if (node.jjtGetNumChildren() == 0) {
            throw new DatawaveFatalQueryException("QueryTree was empty");
        }
    }
}
