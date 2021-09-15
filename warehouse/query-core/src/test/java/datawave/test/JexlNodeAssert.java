package datawave.test;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.assertj.core.api.AbstractAssert;

import java.util.Arrays;
import java.util.Objects;

/**
 * This class provides the ability to perform a number of assertions specific to {@link JexlNode} instances, and is intended to be used for testing purposes.
 */
public class JexlNodeAssert extends AbstractAssert<JexlNodeAssert,JexlNode> {
    
    /**
     * Return a new {@link JexlNodeAssert} that will perform assertions on the specified node.
     * 
     * @param node
     *            the node
     * @return a new {@link JexlNodeAssert} for the node
     */
    public static JexlNodeAssert assertThat(JexlNode node) {
        return new JexlNodeAssert(node, JexlNodeAssert.class);
    }
    
    protected JexlNodeAssert(JexlNode node, Class<?> selfType) {
        super(node, selfType);
    }
    
    /**
     * Verifies that the actual node's image is null
     * 
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert hasNullImage() {
        isNotNull();
        if (actual.image != null) {
            failWithMessage("Expected image to be null, but was %s", actual.image);
        }
        return this;
    }
    
    /**
     * Verifies that the actual node's image is not null.
     * 
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert hasNonNullImage() {
        isNotNull();
        if (actual.image == null) {
            failWithMessage("Expected image to not be null");
        }
        return this;
    }
    
    /**
     * Verifies that the actual node's image is equal to the given one.
     * 
     * @param image
     *            the image
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert hasImage(String image) {
        isNotNull();
        if (!Objects.equals(actual.image, image)) {
            failWithMessage("Expected image to be %s but was %s", image, actual.image);
        }
        return this;
    }
    
    /**
     * Verifies that the actual node's value is null.
     * 
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert hasNullValue() {
        isNotNull();
        if (actual.jjtGetValue() != null) {
            failWithMessage("Expected value to be null, but was %s", actual.jjtGetValue());
        }
        return this;
    }
    
    /**
     * Verifies that the actual node's value is not null.
     * 
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert hasNonNullValue() {
        isNotNull();
        if (actual.jjtGetValue() == null) {
            failWithMessage("Expected value to not be null");
        }
        return this;
    }
    
    /**
     * Verifies that the actual node's value is equal to the given one.
     * 
     * @param value
     *            the value
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert hasValue(Object value) {
        isNotNull();
        if (!Objects.equals(actual.jjtGetValue(), value)) {
            failWithMessage("Expected value to be %s, but was %s", value, actual.jjtGetValue());
        }
        return this;
    }
    
    /**
     * Verifies that the actual node's parent is null.
     * 
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert hasNullParent() {
        isNotNull();
        if (actual.jjtGetParent() != null) {
            failWithMessage("Expected parent to be null");
        }
        return this;
    }
    
    /**
     * Verifies that the actual node's parent is not null.
     * 
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert hasNonNullParent() {
        isNotNull();
        if (actual.jjtGetParent() == null) {
            failWithMessage("Expected parent to not be null, but was %s", actual.jjtGetParent());
        }
        return this;
    }
    
    /**
     * Returns a new {@link JexlNodeAssert} that will perform assertions on the actual node's parent.
     * 
     * @return the new {@link JexlNodeAssert} for the parent
     */
    public JexlNodeAssert parent() {
        isNotNull();
        return assertThat(actual.jjtGetParent());
    }
    
    /**
     * Verifies that the actual node has no children.
     * 
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert hasNoChildren() {
        isNotNull();
        if (actual.jjtGetNumChildren() != 0) {
            failWithMessage("Expected no children, but had children %s", formatChildren(actual));
        }
        return this;
    }
    
    /**
     * Verifies that the actual node has the given number of children.
     * 
     * @param numChildren
     *            the number of children
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert hasNumChildren(int numChildren) {
        isNotNull();
        if (actual.jjtGetNumChildren() != numChildren) {
            failWithMessage("Expected %d children, but had %d", numChildren, actual.jjtGetNumChildren());
            
        }
        return this;
    }
    
    /**
     * Returns a new {@link JexlNodeAssert} that will perform assertions on the specified child.
     * 
     * @param child
     *            the child
     * @return the new {@link JexlNodeAssert} for the child
     */
    public JexlNodeAssert child(int child) {
        isNotNull();
        return assertThat(actual.jjtGetChild(child));
    }
    
    /**
     * Verifies that the actual node's query string as supplied by {@link JexlStringBuildingVisitor#buildQuery(JexlNode)} is an exact match to the given one.
     * 
     * @param query
     *            the query string
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert hasExactQueryString(String query) {
        isNotNull();
        String queryString = getQueryString(actual);
        if (!queryString.equals(query)) {
            failWithMessage("Expected query string to be %s, but was %s", query, queryString);
        }
        return this;
    }
    
    /**
     * Verifies that the actual node's query string as supplied by {@link JexlStringBuildingVisitor#buildQuery(JexlNode)} contains the given expression within
     * it as a substring.
     * 
     * @param expression
     *            the query expression
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert containsWithinQueryString(String expression) {
        isNotNull();
        String queryString = getQueryString(actual);
        if (!queryString.contains(expression)) {
            failWithMessage("Expected query to contain expression %s, query: %s", expression, queryString);
        }
        return this;
    }
    
    /**
     * Verifies that the actual node has a valid lineage as determined by {@link JexlASTHelper#validateLineageVerbosely(JexlNode, boolean)}.
     * 
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert hasValidLineage() {
        isNotNull();
        JexlASTHelper.LineageValidation validation = JexlASTHelper.validateLineageVerbosely(actual, false);
        if (!validation.isValid()) {
            failWithMessage("Expected a valid lineage, but found the following conflicts: \n" + validation.getFormattedInvalidations());
        }
        return this;
    }
    
    /**
     * Verifies that the actual node is equal to the given query string as determined by a {@link TreeEqualityVisitor} comparison. The query string will be
     * parsed to a query tree that will always have a root {@link ASTJexlScript} node.
     * 
     * @param query
     *            the query
     * @return this {@link JexlNodeAssert}
     * @throws ParseException
     *             if the given query string cannot be parsed to a {@link ASTJexlScript}
     */
    public JexlNodeAssert isEqualTo(String query) throws ParseException {
        return isEqualTo(query, false);
    }
    
    /**
     * Verifies that the actual node is equal to the given node as determined by a {@link TreeEqualityVisitor} comparison.
     * 
     * @param node
     *            the node
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert isEqualTo(JexlNode node) {
        return isEqualTo(node, false);
    }
    
    /**
     * Verifies that the actual node is equal to the given query string as determined by a {@link TreeEqualityVisitor} comparison. The query string will be
     * parsed to a query tree that will always have a root {@link ASTJexlScript} node. If the assertion fails and printQueries is true, both the actual node and
     * parsed node will be printed to the system output via {@link PrintingVisitor#printQuery(String)} for debugging purposes.
     * 
     * @param query
     *            the query
     * @return this {@link JexlNodeAssert}
     * @throws ParseException
     *             if the given query string cannot be parsed to a {@link ASTJexlScript}
     */
    public JexlNodeAssert isEqualTo(String query, boolean printQueries) throws ParseException {
        isNotNull();
        ASTJexlScript expected = JexlASTHelper.parseJexlQuery(query);
        assertEqual(actual, expected, printQueries);
        return this;
    }
    
    /**
     * Verifies that the actual node is equal to the given node as determined by a {@link TreeEqualityVisitor} comparison. If the assertion fails and
     * printQueries is true, both the actual node and parsed node will be printed to the system output via {@link PrintingVisitor#printQuery(String)} for
     * debugging purposes.
     * 
     * @param node
     *            the node
     * @return this {@link JexlNodeAssert}
     */
    public JexlNodeAssert isEqualTo(JexlNode node, boolean printQueries) {
        if (node != actual) {
            if (node == null || actual == null) {
                failWithMessage("Expected actual to be " + getQueryString(node) + " but was " + getQueryString(actual));
            } else {
                assertEqual(actual, node, printQueries);
            }
        }
        return this;
    }
    
    // Return a comma-delimited list of the node's children.
    private String formatChildren(JexlNode node) {
        return Arrays.toString(getChildren(node));
    }
    
    // Return a copy of the node's children array.
    private JexlNode[] getChildren(JexlNode node) {
        int numChildren = node.jjtGetNumChildren();
        JexlNode[] children = new JexlNode[numChildren];
        for (int i = 0; i < numChildren; i++) {
            children[i] = node.jjtGetChild(i);
        }
        return children;
    }
    
    // Return the query tree's query string, or "null" if the query tree is null.
    private String getQueryString(JexlNode node) {
        return node != null ? JexlStringBuildingVisitor.buildQuery(node) : "null";
    }
    
    // Assert whether the two nodes are equal.
    private void assertEqual(JexlNode actual, JexlNode expected, boolean printQueries) {
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expected, actual);
        if (!comparison.isEqual()) {
            failWithMessage("Expected actual to be " + getQueryString(expected) + " but was non-equal: " + comparison.getReason());
            if (printQueries) {
                PrintingVisitor.printQuery(actual);
                PrintingVisitor.printQuery(expected);
            }
        }
    }
}
